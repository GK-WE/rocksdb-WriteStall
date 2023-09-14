//
// Created by Haoyu Gong on 2023.
//

#include "input_rate_controller.h"
#include "db/column_family.h"
#include "db/version_set.h"
#include "logging/logging.h"

namespace ROCKSDB_NAMESPACE{
const int64_t low_bkop_max_wait_us = 100000; // 1/10 sec
//const int64_t low_dlcmp_max_wait_us = 200000; // 4/10 sec
struct InputRateController::Req {
  explicit Req(int64_t _bytes, port::Mutex* _mu, Env::BackgroundOp _background_op,
               int _cc_when_blocked, int _cushion_when_blocked,ThreadSignaledReason _signaled_reason, Env::BackgroundOp _signaled_by_op)
  : request_bytes(_bytes),
        bytes(_bytes),
        cv(_mu),
        background_op(_background_op),
        cc_when_blocked(_cc_when_blocked),
        cushion_when_blocked(_cushion_when_blocked),
        signaled_reason(_signaled_reason),
        signaled_by_op(_signaled_by_op){}
  int64_t request_bytes;
  int64_t bytes;
  port::CondVar cv;
  Env::BackgroundOp background_op;
  int cc_when_blocked;
  int cushion_when_blocked;
  ThreadSignaledReason signaled_reason;
  Env::BackgroundOp signaled_by_op; //if req is blocked which op signaled it
};

struct InputRateController::InfoCFD{
  explicit InfoCFD(std::string _name, VersionStorageInfo* _vstorage, int _mem,
                   const MutableCFOptions& _mutable_cf_options)
                   :name(_name),
                    vstorage(_vstorage),
                    mem(_mem),
                    mutable_cf_options(_mutable_cf_options){}
  std::string name;
  VersionStorageInfo* vstorage;
  int mem;
  const MutableCFOptions& mutable_cf_options;
};

InputRateController::InputRateController()
: clock_(SystemClock::Default()),
  cur_high_(0),
  exit_cv_(&request_mutex_),
  requests_to_wait_(0),
  stop_(false),
  need_debug_info_(false){}

InputRateController::~InputRateController() {
  MutexLock g(&request_mutex_);
  stop_ = true;
  std::deque<Req*>::size_type queues_size_sum = 0;
  for(auto& stopped_bkop_queue : stopped_bkop_queue_){
    for (int i = Env::BK_FLUSH; i < Env::BK_TOTAL; ++i) {
      queues_size_sum += stopped_bkop_queue.second[i].size();
    }
  }

  for(auto& low_bkop_queue : low_bkop_queue_){
    queues_size_sum += low_bkop_queue.second.size();
  }

  requests_to_wait_ = static_cast<int32_t>(queues_size_sum);

  for(auto& stopped_bkop_queue : stopped_bkop_queue_) {
    for (int i = Env::BK_TOTAL - 1; i >= Env::BK_FLUSH; --i) {
      std::deque<Req*> queue = stopped_bkop_queue.second[i];
      while (!queue.empty()) {
        queue.front()->signaled_reason = TSREASON_SHUTDOWN;
        queue.front()->cv.Signal();
        queue.pop_front();
      }
    }
  }
  for(auto& low_bkop_queue : low_bkop_queue_) {
    for (auto& r : low_bkop_queue.second) {
      r->cv.Signal();
    }
  }

  while(requests_to_wait_ > 0){
    exit_cv_.Wait();
  }
}

void InputRateController::SetCurCFDInfo(ColumnFamilyData* cfd, VersionStorageInfo* vstorage, int mem, const MutableCFOptions& mutable_cf_options) {
  MutexLock g(&request_mutex_);
  std::string name = cfd->GetName();
  InfoCFD* new_cfd_info = new InfoCFD(name,vstorage,mem,mutable_cf_options);
  int pre_pre_ec = 0;
  int pre_ec = 0;
  int cur_ec = 0;
  if(cur_cfd_info.find(name)==cur_cfd_info.end()){
    cur_cfd_info[name] = new_cfd_info;
    prev_cfd_info[name] = nullptr;
  }else{
    if(prev_cfd_info[name] != nullptr){
      pre_pre_ec = prev_cfd_info[name]->vstorage->estimated_compaction_needed_bytes() >> 30;
      delete prev_cfd_info[name];
    }
    prev_cfd_info[name] = cur_cfd_info[name];
    cur_cfd_info[name] = new_cfd_info;
    pre_ec = prev_cfd_info[name]->vstorage->estimated_compaction_needed_bytes() >> 30;
    cur_ec = cur_cfd_info[name]->vstorage->estimated_compaction_needed_bytes() >> 30;

    ROCKS_LOG_INFO(cfd->ioptions()->logger, "[%s] ------------------UpdateCFDInfo------------------: "
                                            "pre-pre-ECsize: %d (GB) "
                                            "pre-ECsize: %d (GB) "
                                            "cur-ECsize: %d (GB) ",cfd->GetName().c_str(),
                                            pre_pre_ec,
                                            pre_ec,
                                            cur_ec);
  }
  UpdateCC(cfd);
  UpdateCushion(cfd);
  UpdateBackgroundOpPri(cfd);
//  UpdateStoppedOp();
}

void InputRateController::UpdateCC(ColumnFamilyData* cur_cfd) {
  request_mutex_.AssertHeld();
  for(auto &cfd : cur_cfd_info){
    if(cfd.first != cur_cfd->GetName()){
      continue;
    }
    int pre_pre_cc;
    if(cur_cc.find(cfd.first) != cur_cc.end()){
      pre_pre_cc = prev_cc[cfd.first];
      prev_cc[cfd.first] = cur_cc[cfd.first];
    }else{
      pre_pre_cc = CC_NORMAL;
      prev_cc[cfd.first] = CC_NORMAL;
    }
    const MutableCFOptions& mutable_cf_options = cfd.second->mutable_cf_options;
    int num_unflushed_memtables = cfd.second->mem;
    int num_l0_sst =  cfd.second->vstorage->l0_delay_trigger_count();
    uint64_t estimated_compaction_needed_bytes = cfd.second->vstorage->estimated_compaction_needed_bytes();
    bool MT = (num_unflushed_memtables >= mutable_cf_options.max_write_buffer_number);
    bool L0 = (num_l0_sst >= mutable_cf_options.level0_slowdown_writes_trigger);
    bool EC = (estimated_compaction_needed_bytes >=
        (mutable_cf_options.hard_pending_compaction_bytes_limit));
    cur_cc[cfd.first] = (MT ? 1 : 0) + (L0 ? 2 : 0) + (EC ? 4 : 0);
    ROCKS_LOG_INFO(cur_cfd->ioptions()->logger, "[%s] UpdateCFDInfo-UpdateCC: "
                                            "pre-pre-CC: %s "
                                            "pre-CC: %s "
                                            "cur-CC: %s ",cur_cfd->GetName().c_str(),
                   CCConditionString(pre_pre_cc).c_str(),
                   CCConditionString(prev_cc[cfd.first]).c_str(),
                   CCConditionString(cur_cc[cfd.first]).c_str());
  }
}

void InputRateController::UpdateCushion(ColumnFamilyData* cur_cfd) {
  request_mutex_.AssertHeld();
  for(auto &cfd : cur_cfd_info){
    if(cfd.first != cur_cfd->GetName()){
      continue;
    }
    int  current_cc = cur_cc[cfd.first];
    int  previous_cc = prev_cc[cfd.first];

    bool cur_L0 = (current_cc >> 1) & 1;
    bool cur_EC = (current_cc >> 2) & 1;
    bool prev_L0 = (previous_cc >> 1) & 1;
    bool prev_EC = (previous_cc >> 2) & 1;

    int previous_cushion;
    if(cushion.find(cfd.first)!=cushion.end()){
      previous_cushion = cushion[cfd.first];
    }else{
      previous_cushion = CUSHION_NO_EC_NO_L0;
    }

    bool l0_decrease_to_safe = false;
    bool ec_decrease_to_safe = false;
    int l0_sst_num = cfd.second->vstorage->l0_delay_trigger_count();
    int l0_sst_limit = 0;
    if(l0_sst_num <= l0_sst_limit){
      l0_decrease_to_safe = true;
    }
    uint64_t cmp_bytes_needed = cfd.second->vstorage->estimated_compaction_needed_bytes();
    uint64_t cmp_bytes_limit = cfd.second->mutable_cf_options.hard_pending_compaction_bytes_limit;
    if(cmp_bytes_needed <= (uint64_t)(cmp_bytes_limit*(1/2))){
      ec_decrease_to_safe = true;
    }

    int cur_L0_cushion = 0;
    int pre_L0_cushion = (previous_cushion & 7);
    int cur_EC_cushion = 0;
    int pre_EC_cushion = ((previous_cushion >> 4) & 7);
    if(cur_L0){
      //check if previous is also L0-CC
      if(prev_L0){
        //if previous also
        cur_L0_cushion = CUSHION_STATE_KEEP; //KEEP_L0
      }else{
        cur_L0_cushion = CUSHION_STATE_INC; //INC_L0
      }
    }else{
      //current is below L0 limit
      //check if it is in decrease stage but not to
      if(pre_L0_cushion == CUSHION_STATE_NORMAL){
        cur_L0_cushion = 0;
      }else if(l0_decrease_to_safe){
        cur_L0_cushion = 0;
      }else{
        cur_L0_cushion = CUSHION_STATE_DEC;
      }
    }
    if(cur_EC){
      //check if previous is also EC-CC
      if(prev_EC){
        //if previous also
        cur_EC_cushion = CUSHION_STATE_KEEP; //KEEP_L0
      }else{
        cur_EC_cushion = CUSHION_STATE_INC; //INC_L0
      }
    }else{
      //current is below EC limit
      //check if it is in decrease stage but not to
      if(pre_EC_cushion == CUSHION_STATE_NORMAL){
        cur_EC_cushion = 0;
      }else if(ec_decrease_to_safe){
        cur_EC_cushion = 0;
      }else{
        cur_EC_cushion = CUSHION_STATE_DEC;
      }
    }

    cushion[cfd.first] = cur_L0_cushion + (cur_EC_cushion << 4);

    ROCKS_LOG_INFO(cur_cfd->ioptions()->logger, "[%s] UpdateCFDInfo-UpdateCushion: "
                                                "pre-Cushion(pre_pre_CC~~pre_CC): %s "
                                                "cur-Cushion(pre_CC~~cur_CC): %s ",cur_cfd->GetName().c_str(),
                                                CushionString(previous_cushion).c_str(),
                                                CushionString(cushion[cfd.first]).c_str());
  }
}

void InputRateController::UpdateBackgroundOpPri(ColumnFamilyData* cur_cfd) {
  request_mutex_.AssertHeld();
  for(auto &cfd : cur_cfd_info){
    if(cfd.first != cur_cfd->GetName()){
      continue;
    }
    std::string name = cfd.first;
    int current_cc;
    int current_cushion;
    if(cur_cc.find(name) == cur_cc.end()){
      current_cc = CC_NORMAL;
      current_cushion = CUSHION_NO_EC_NO_L0;
    }else{
      current_cc = cur_cc[name];
      current_cushion = cushion[name];
    }
    int current_L0_cushion = current_cushion & 7;
    int current_EC_cushion = (current_cushion >> 4) & 7;
    switch (current_cc) {
      case CC_NORMAL:
        if(current_cushion == 0 ){
          op_pri[name][Env::BK_FLUSH] = IO_TOTAL;
          op_pri[name][Env::BK_L0CMP] = IO_HIGH;
          op_pri[name][Env::BK_DLCMP] = IO_LOW;
        }else if(current_L0_cushion){
          op_pri[name][Env::BK_FLUSH] = IO_STOP;
          op_pri[name][Env::BK_L0CMP] = IO_HIGH;
          op_pri[name][Env::BK_DLCMP] = IO_LOW;
        }else if(current_EC_cushion){
          op_pri[name][Env::BK_FLUSH] = IO_LOW;
          op_pri[name][Env::BK_L0CMP] = IO_STOP;
          op_pri[name][Env::BK_DLCMP] = IO_HIGH;
        }
        break;
      case CC_MT:
        if(current_cushion == 0 ){
          op_pri[name][Env::BK_FLUSH] = IO_HIGH;
          op_pri[name][Env::BK_L0CMP] = IO_LOW;
          op_pri[name][Env::BK_DLCMP] = IO_LOW;
        }else if(current_L0_cushion){
          op_pri[name][Env::BK_FLUSH] = IO_STOP;
          op_pri[name][Env::BK_L0CMP] = IO_HIGH;
          op_pri[name][Env::BK_DLCMP] = IO_LOW;
        }else if(current_EC_cushion){
          op_pri[name][Env::BK_FLUSH] = IO_LOW;
          op_pri[name][Env::BK_L0CMP] = IO_STOP;
          op_pri[name][Env::BK_DLCMP] = IO_HIGH;
        }
        break;
      case CC_L0:
        op_pri[name][Env::BK_FLUSH] = IO_STOP;
        op_pri[name][Env::BK_L0CMP] = IO_HIGH;
        op_pri[name][Env::BK_DLCMP] = IO_LOW;
        break;
      case CC_L0MT:
        op_pri[name][Env::BK_FLUSH] = IO_STOP;
        op_pri[name][Env::BK_L0CMP] = IO_HIGH;
        op_pri[name][Env::BK_DLCMP] = IO_LOW;
        break;
      case CC_EC:
        op_pri[name][Env::BK_FLUSH] = IO_LOW;
        op_pri[name][Env::BK_L0CMP] = IO_STOP;
        op_pri[name][Env::BK_DLCMP] = IO_HIGH;
        break;
      case CC_ECMT:
        op_pri[name][Env::BK_FLUSH] = IO_LOW;
        op_pri[name][Env::BK_L0CMP] = IO_STOP;
        op_pri[name][Env::BK_DLCMP] = IO_HIGH;
        break;
      case CC_ECL0:
        op_pri[name][Env::BK_FLUSH] = IO_STOP;
        op_pri[name][Env::BK_L0CMP] = IO_HIGH;
        op_pri[name][Env::BK_DLCMP] = IO_LOW;
        break;
      case CC_ECL0MT:
        op_pri[name][Env::BK_FLUSH] = IO_STOP;
        op_pri[name][Env::BK_L0CMP] = IO_HIGH;
        op_pri[name][Env::BK_DLCMP] = IO_LOW;
      break;
      default:
        op_pri[name][Env::BK_FLUSH] = IO_TOTAL;
        op_pri[name][Env::BK_L0CMP] = IO_HIGH;
        op_pri[name][Env::BK_DLCMP] = IO_LOW;
      break;
    }
  }
}

InputRateController::BackgroundOp_Priority InputRateController::DecideBackgroundOpPriority(Env::BackgroundOp background_op,std::string name){
//  request_mutex_.AssertHeld();
  InputRateController::BackgroundOp_Priority pri;
  if(op_pri.find(name)!=op_pri.end()){
    pri = op_pri[name][background_op];
  }else{
    pri = (background_op == Env::BK_FLUSH) ? IO_TOTAL: ((background_op == Env::BK_L0CMP) ? IO_HIGH : IO_LOW);
  }
  return pri;
}

size_t InputRateController::RequestToken(size_t bytes, size_t alignment,
                                         ColumnFamilyData* cfd,
                                         Env::BackgroundOp background_op,
                                         bool& need_return_token) {
  if (alignment > 0) {
    bytes = std::max(alignment, TruncateToPageBoundary(alignment, bytes));
  }
  Request(bytes,cfd,background_op,need_return_token);
  return bytes;
}

void InputRateController::Request(size_t bytes, ColumnFamilyData* cfd,
                                  Env::BackgroundOp background_op,
                                  bool& need_return_token) {

  std::string cfd_name = cfd->GetName();
  InputRateController::BackgroundOp_Priority pri = DecideBackgroundOpPriority(background_op,cfd_name);
  need_return_token = false;
  if(pri==IO_TOTAL) {
    return;
  }
  MutexLock g(&request_mutex_);
  int current_cc = cur_cc[cfd_name];
  int current_cushion = cushion[cfd_name];
  if(need_debug_info_){
    ROCKS_LOG_INFO(cfd->ioptions()->logger,"[%s] RequestToken-Enter: "
                                           "background_op: %s "
                                           "io_pri: %s "
                                           "bytes: %zu "
                                           "cc_cur: %s "
                                           "cc_cushion: %s ",cfd->GetName().c_str(),
                                           BackgroundOpString(background_op).c_str(),
                                           BackgroundOpPriorityString(pri).c_str(),
                                           bytes,
                                           CCConditionString(current_cc).c_str(),
                                           CushionString(current_cushion).c_str());
  }



  if(need_debug_info_){
    ROCKS_LOG_INFO(cfd->ioptions()->logger,"[%s] RequestToken-GotMutex: "
                                           "background_op: %s "
                                           "io_pri: %s "
                                           "bytes: %zu "
                                           "cc_cur: %s "
                                           "cc_cushion: %s ",cfd->GetName().c_str(),
                                           BackgroundOpString(background_op).c_str(),
                                           BackgroundOpPriorityString(pri).c_str(), bytes,
                                           CCConditionString(current_cc).c_str(),
                                           CushionString(current_cushion).c_str());
  }

//  SignalStopOpExcept(cfd,stopped_op,background_op,io_pri);

if(pri == IO_STOP){
    Req r(bytes, &request_mutex_, background_op, current_cc, current_cushion,TSREASON_TOTAL, Env::BK_NONE);
    stopped_bkop_queue_[cfd_name][background_op].push_back(&r);
    if(need_debug_info_){
      ROCKS_LOG_INFO(cfd->ioptions()->logger,"[%s] RequestToken-StartWait: "
                                             "background_op: %s "
                                             "io_pri: %s "
                                             "bytes: %zu "
                                             "cc_cur: %s "
                                             "cc_cushion: %s "
                                             "req: %p ", cfd->GetName().c_str(),
                                             BackgroundOpString(background_op).c_str(),
                                             BackgroundOpPriorityString(pri).c_str(),
                                             bytes,
                                             CCConditionString(current_cc).c_str(),
                                             CushionString(current_cushion).c_str(),
                                             &r);
    }

    do{
      if(DecideBackgroundOpPriority(background_op,cfd_name) != IO_STOP){
        r.signaled_reason = TSREASON_CC_CHANGE;
        break;
      }
      int64_t wait_until = clock_->NowMicros() + low_bkop_max_wait_us;
      r.cv.TimedWait(wait_until);
    }while(!stop_);

    if(need_debug_info_){
      ROCKS_LOG_INFO(cfd->ioptions()->logger,"[%s] RequestToken-FinishWait-Signaled: "
                                             "backgroundop: %s "
                                             "io_pri: %s "
                                             "bytes: %zu "
                                             "cc_when_req_issue: %s "
                                              "cushion_when_req_issue: %s "
                                             "cc_when_req_signaled: %s "
                                             "req: %p "
                                             "signaled_reason: %s "
                                             "signaled_by_op: %s ", cfd->GetName().c_str(),
                                             BackgroundOpString(background_op).c_str(),
                                             BackgroundOpPriorityString(pri).c_str(),
                                             bytes,
                                             CCConditionString(current_cc).c_str(),
                                             CushionString(current_cushion).c_str(),
                                             CCConditionString(cur_cc[cfd_name]).c_str(),
                                             &r,
                                             TSReasonString(r.signaled_reason).c_str(),
                                             BackgroundOpString(r.signaled_by_op).c_str());
    }

    stopped_bkop_queue_[cfd_name][background_op].erase(std::remove(stopped_bkop_queue_[cfd_name][background_op].begin(),
                                                                   stopped_bkop_queue_[cfd_name][background_op].end(), &r),
                                                       stopped_bkop_queue_[cfd_name][background_op].end());
  }else if(pri == IO_HIGH){
  need_return_token = true;
    ++cur_high_;
  }else if(pri == IO_LOW){
    Req r(bytes, &request_mutex_, background_op,current_cc,current_cushion,TSREASON_TOTAL, Env::BK_NONE);
    low_bkop_queue_[cfd_name].push_back(&r);
    bool timeout_occurred = false;
    if(need_debug_info_){
      ROCKS_LOG_INFO(cfd->ioptions()->logger,"[%s] RequestToken-StartWait: "
                                             "background_op: %s "
                                             "io_pri: %s "
                                             "bytes: %zu "
                                             "cc_cur: %s "
                                             "cc_cushion: %s "
                                             "req: %p", cfd->GetName().c_str(),
                                             BackgroundOpString(background_op).c_str(),
                                             BackgroundOpPriorityString(pri).c_str(),
                                             bytes,
                                             CCConditionString(current_cc).c_str(),
                                             CushionString(current_cushion).c_str(),
                                             &r);
    }

    do{
      if(cur_high_.load(std::memory_order_relaxed)==0){
        r.signaled_reason = TSREASON_ZERO_HIGH;
        break;
      }
      if(DecideBackgroundOpPriority(background_op,cfd_name)==IO_HIGH){
        r.signaled_reason = TSREASON_CC_CHANGE;
        break;
      }
      int64_t wait_until = clock_->NowMicros() + low_bkop_max_wait_us;
      timeout_occurred = r.cv.TimedWait(wait_until);
    }while(!timeout_occurred);

    if(timeout_occurred){
      r.signaled_reason = TSREASON_TIMEOUT;
    }

    if(need_debug_info_){
      ROCKS_LOG_INFO(cfd->ioptions()->logger,"[%s] RequestToken-FinishWait-Signaled: "
                                             "backgroundop: %s "
                                             "io_pri: %s "
                                             "bytes: %zu "
                                             "cc_when_req_issue: %s "
                                             "cushion_when_req_issue: %s "
                                             "cc_when_req_signaled: %s "
                                             "req: %p "
                                             "signaled_reason: %s "
                                             "signaled_by_op: %s ", cfd->GetName().c_str(),
                                             BackgroundOpString(background_op).c_str(),
                                             BackgroundOpPriorityString(pri).c_str(),
                                             bytes,
                                             CCConditionString(current_cc).c_str(),
                     CushionString(current_cushion).c_str(),
                                             CCConditionString(cur_cc[cfd_name]).c_str(),
                                             &r,
                                             TSReasonString(r.signaled_reason).c_str(),
                                             BackgroundOpString(r.signaled_by_op).c_str());
    }


    // When LOW thread is signaled, it should be removed from low_bkop_queue_ by itself
    // Make sure don't iterate low_bkop_queue_ from another thread while trying to signal LOW threads in low_bkop_queue_
    low_bkop_queue_[cfd_name].erase(std::remove(low_bkop_queue_[cfd_name].begin(), low_bkop_queue_[cfd_name].end(), &r), low_bkop_queue_[cfd_name].end());
  }

  if(stop_){
    --requests_to_wait_;
    if(need_debug_info_){
      ROCKS_LOG_INFO(cfd->ioptions()->logger,"[%s] RequestToken-Decrease-requests_to_wait: "
                                             "background_op: %s "
                                             "io_pri: %s "
                                             "bytes: %zu "
                                             "cc_when_req_issue: %s "
                                             "cushion_when_req_issue: %s "
                                             "requests_to_wait: %d ",cfd->GetName().c_str(),
                                             BackgroundOpString(background_op).c_str(),
                                             BackgroundOpPriorityString(pri).c_str(),
                                             bytes,
                                             CCConditionString(current_cc).c_str(),
                                             CushionString(current_cushion).c_str(),
                                             requests_to_wait_);
    }

    exit_cv_.Signal();
  }

  if(need_debug_info_){
    ROCKS_LOG_INFO(cfd->ioptions()->logger,"[%s] RequestToken-ReleaseMutex: "
                                           "background_op: %s "
                                           "io_pri: %s "
                                           "bytes: %zu "
                                           "cc_when_req_issue: %s "
                                           "cushion_when_req_issue: %s ",cfd->GetName().c_str(),
                                           BackgroundOpString(background_op).c_str(),
                                           BackgroundOpPriorityString(pri).c_str(),
                                           bytes,
                                           CCConditionString(current_cc).c_str(),
                                           CushionString(current_cushion).c_str());
  }

}

void InputRateController::ReturnToken(ColumnFamilyData* cfd, Env::BackgroundOp background_op) {
  if(need_debug_info_){
    ROCKS_LOG_INFO(cfd->ioptions()->logger,"[%s] ReturnToken-Enter: "
                                           "background_op: %s "
                                           "io_pri: HIGH ", cfd->GetName().c_str(),
                                           BackgroundOpString(background_op).c_str());
  }

  MutexLock g(&request_mutex_);
  std::string cfd_name = cfd->GetName();
  if(need_debug_info_){
    ROCKS_LOG_INFO(cfd->ioptions()->logger,"[%s] ReturnToken-GotMutex: "
                                           "background_op: %s "
                                           "io_pri: HIGH ", cfd->GetName().c_str(),
                                           BackgroundOpString(background_op).c_str());
  }

  --cur_high_;
  if(cur_high_.load(std::memory_order_relaxed)==0 && !low_bkop_queue_[cfd_name].empty()){
    Req* r = low_bkop_queue_[cfd_name].front();
    if(need_debug_info_){
      ROCKS_LOG_INFO(cfd->ioptions()->logger,"[%s] ReturnToken-StartSignal-LOWop: "
                                             "background_op: %s "
                                             "io_pri: HIGH "
                                             "req: %p "
                                             "signal_reason: TSREASON_ZERO_HIGH "
                                             "signaled_op: %s", cfd->GetName().c_str(),
                                             BackgroundOpString(background_op).c_str(),
                                             r,
                                             BackgroundOpString(r->background_op).c_str());
    }

    r->signaled_reason = TSREASON_ZERO_HIGH;
    r->signaled_by_op = background_op;
    r->cv.Signal();
    if(need_debug_info_){
      ROCKS_LOG_INFO(cfd->ioptions()->logger,"[%s] ReturnToken-FinishSignal-LOWop: "
                                             "background_op: %s "
                                             "io_pri: HIGH "
                                             "req: %p "
                                             "signal_reason: TSREASON_ZERO_HIGH "
                                             "signaled_op: %s", cfd->GetName().c_str(),
                                             BackgroundOpString(background_op).c_str(),
                                             r,
                                             BackgroundOpString(r->background_op).c_str());
    }

  }

  if(need_debug_info_){
    ROCKS_LOG_INFO(cfd->ioptions()->logger,"[%s] ReturnToken-ReleaseMutex: "
                                           "background_op: %s io_pri: HIGH", cfd->GetName().c_str(),
                                           BackgroundOpString(background_op).c_str());
  }

}

std::string InputRateController::BackgroundOpPriorityString(BackgroundOp_Priority io_pri) {
  std::string res;
  switch (io_pri) {
    case IO_STOP: res = "STOP";
    break;
    case IO_LOW: res = "LOW";
    break;
    case IO_HIGH: res = "HIGH";
    break;
    case IO_TOTAL: res = "TOTAL";
    break;
    default: res = "NA";
    break;
  }
  return res;
}

std::string InputRateController::BackgroundOpString(Env::BackgroundOp op) {
  std::string res;
  switch (op) {
    case Env::BK_FLUSH: res = "FLUSH";
    break;
    case Env::BK_L0CMP: res = "L0CMP";
    break;
    case Env::BK_DLCMP: res = "DLCMP";
    break;
    case Env::BK_TOTAL: res = "OTHER";
    break;
    default: res = "NA";
    break;
  }
  return res;
}

std::string InputRateController::CCConditionString(int cc) {
  std::string res;
  switch (cc) {
    case CC_NORMAL: res = "CC_NORMAL";
      break;
      case CC_MT: res = "CC_MT";
      break;
      case CC_L0: res = "CC_L0";
      break;
      case CC_EC: res = "CC_EC";
      break;
      case CC_L0MT: res = "CC_L0MT";
      break;
      case CC_ECMT: res = "CC_ECMT";
      break;
      case CC_ECL0: res = "CC_ECL0";
      break;
      case CC_ECL0MT: res = "CC_ECL0MT";
      break;
    default: res = "NA";
      break;
  }
  return res;
}

std::string InputRateController::CushionString(int cu) {
  std::string res = "CUSHION_";
  int L0_cushion = (cu & 7) ;
  int EC_cushion = (cu >> 4) & 7;

  switch (EC_cushion) {
    case CUSHION_STATE_NORMAL:
      res += "NO_EC_"; break;
    case CUSHION_STATE_INC:
      res += "INC_EC_"; break;
      case CUSHION_STATE_DEC:
        res += "DEC_EC_"; break;
        case CUSHION_STATE_KEEP:
          res += "KEEP_EC_"; break;
  }
  switch (L0_cushion) {
    case CUSHION_STATE_NORMAL:
      res += "NO_L0"; break;
      case CUSHION_STATE_INC:
        res += "INC_L0"; break;
        case CUSHION_STATE_DEC:
          res += "DEC_L0"; break;
          case CUSHION_STATE_KEEP:
            res += "KEEP_L0"; break;
  }
  return res;
}

std::string InputRateController::TSReasonString(int ts) {
  std::string res;
  switch (ts) {
    case TSREASON_CC_CHANGE:
      res = "TSREASON_CC_CHANGE"; break;
    case TSREASON_ZERO_HIGH:
      res = "TSREASON_ZERO_HIGH"; break;
    case TSREASON_TIMEOUT:
      res = "TSREASON_TIMEOUT"; break;
    case TSREASON_SHUTDOWN:
      res = "TSREASON_SHUTDOWN"; break;
    default: res = "NA"; break;
  }
  return res;
}

InputRateController* NewInputRateController(){
  std::unique_ptr<InputRateController> input_rate_controller(new InputRateController());
  return input_rate_controller.release();
}

}