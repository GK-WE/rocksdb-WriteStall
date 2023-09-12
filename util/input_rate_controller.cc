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

void InputRateController::SetCurCFDInfo(std::string name, VersionStorageInfo* vstorage, int mem, const MutableCFOptions& mutable_cf_options) {
  MutexLock g(&request_mutex_);
  InfoCFD* new_cfd_info = new InfoCFD(name,vstorage,mem,mutable_cf_options);
  if(cur_cfd_info.find(name)==cur_cfd_info.end()){
    cur_cfd_info[name] = new_cfd_info;
    prev_cfd_info[name] = nullptr;
  }else{
    if(prev_cfd_info[name] != nullptr){
      delete prev_cfd_info[name];
    }
    prev_cfd_info[name] = cur_cfd_info[name];
    cur_cfd_info[name] = new_cfd_info;
  }
  UpdateCC();
  UpdateCushion();
  UpdateStoppedOp();
}

void InputRateController::UpdateCC() {
  request_mutex_.AssertHeld();
  for(auto &cfd : cur_cfd_info){
    if(cur_cc.find(cfd.first) != cur_cc.end()){
      prev_cc[cfd.first] = cur_cc[cfd.first];
    }else{
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
  }
}

void InputRateController::UpdateCushion() {
  request_mutex_.AssertHeld();
  for(auto &cfd : cur_cfd_info){
    int  current_cc = cur_cc[cfd.first];
    int  previous_cc = prev_cc[cfd.first];

    bool cur_L0 = (current_cc >> 1) & 1;
    bool cur_EC = (current_cc >> 2) & 1;
//    bool prev_L0 = (previous_cc >> 1) & 1;
//    bool prev_EC = (previous_cc >> 2) & 1;

    int previous_cushion;
    if(cushion.find(cfd.first)!=cushion.end()){
      previous_cushion = cushion[cfd.first];
    }else{
      previous_cushion = CUSHION_NORMAL;
    }
    cushion[cfd.first] = CUSHION_NORMAL;

    bool maybe_l0_restore_stage = (previous_cushion == CUSHION_L0) ||
        (previous_cushion == CUSHION_ECL0);
    bool maybe_dl_restore_stage = (previous_cushion == CUSHION_EC) ||
        (previous_cushion == CUSHION_ECL0);

    if((previous_cc == current_cc || previous_cc == CC_NORMAL)
    && (!maybe_l0_restore_stage)
    && (!maybe_dl_restore_stage)){
      continue;
    }else{
      if(maybe_l0_restore_stage && (!cur_L0)){
        //if L0-CC was previously violated and now is not
        // we should further check if it leaves room for flush
        int l0_sst_num = cfd.second->vstorage->l0_delay_trigger_count();
        int l0_sst_limit = 0;
        if(l0_sst_num > l0_sst_limit){
          cushion[cfd.first] += 1;
        }
      }
      if(maybe_dl_restore_stage && (!cur_EC)){
        //if EC-CC was previously violated and now is not
        // we should further check if it leaves room for L0-L1 cmp
        uint64_t cmp_bytes_needed = cfd.second->vstorage->estimated_compaction_needed_bytes();
        uint64_t cmp_bytes_limit = cfd.second->mutable_cf_options.hard_pending_compaction_bytes_limit;
        if(cmp_bytes_needed > (uint64_t)(cmp_bytes_limit*(1/2))){
          cushion[cfd.first] += 2;
        }
      }
    }
  }
}

void InputRateController::UpdateStoppedOp() {
  request_mutex_.AssertHeld();
  for(auto &cfd : cur_cfd_info){
    int current_cc = cur_cc[cfd.first];
    int current_cushion = cushion[cfd.first];
    Env::BackgroundOp stopped;
    switch (current_cc) {
      case CC_NORMAL:
        switch (current_cushion) {
          case CUSHION_NORMAL: stopped = Env::BK_TOTAL;
          break;
          case CUSHION_L0: stopped = Env::BK_FLUSH;
          break;
          case CUSHION_EC: stopped = Env::BK_FLUSH;
          break;
          case CUSHION_ECL0: stopped = Env::BK_FLUSH;
          break;
          default: stopped = Env::BK_TOTAL;
          break;
        }
        break;
      case CC_MT:
        switch (current_cushion) {
          case CUSHION_NORMAL: stopped = Env::BK_TOTAL;
          break;
          case CUSHION_L0: stopped = Env::BK_FLUSH;
          break;
          case CUSHION_EC: stopped = Env::BK_FLUSH;
          break;
          case CUSHION_ECL0: stopped = Env::BK_FLUSH;
          break;
          default: stopped = Env::BK_TOTAL;
          break;
        }
        break;
      case CC_L0: stopped = Env::BK_FLUSH;
      break;
      case CC_L0MT: stopped = Env::BK_FLUSH;
      break;
      case CC_EC: stopped = Env::BK_FLUSH;
      break;
      case CC_ECMT: stopped = Env::BK_FLUSH;
      break;
      case CC_ECL0: stopped = Env::BK_FLUSH;
      break;
      case CC_ECL0MT: stopped = Env::BK_FLUSH;
      break;
      default: stopped = Env::BK_TOTAL;
      break;
    }
    stopped_op[cfd.first] = stopped;
  }

}

InputRateController::BackgroundOp_Priority InputRateController::DecideBackgroundOpPriority(Env::BackgroundOp background_op,std::string name) {
  request_mutex_.AssertHeld();
  InputRateController::BackgroundOp_Priority io_pri;
  int current_cc;
  int current_cushion;
  if(cur_cc.find(name) == cur_cc.end()){
    current_cc = CC_NORMAL;
    current_cushion = CUSHION_NORMAL;
  }else{
    current_cc = cur_cc[name];
    current_cushion = cushion[name];
  }
  switch (current_cc) {
    case CC_NORMAL:
      switch (current_cushion) {
        case CUSHION_NORMAL: io_pri = (background_op == Env::BK_DLCMP)? IO_LOW: ((background_op == Env::BK_L0CMP)?IO_HIGH:IO_TOTAL);
          break;
          case CUSHION_L0: io_pri = (background_op == Env::BK_FLUSH)? IO_STOP : ((background_op==Env::BK_L0CMP)?IO_HIGH:IO_LOW); //flush stopped
          break;
        case CUSHION_EC: io_pri = (background_op == Env::BK_L0CMP) ? IO_LOW : ((background_op==Env::BK_DLCMP)?IO_HIGH:IO_STOP); //flush stopped
          break;
          case CUSHION_ECL0: io_pri = (background_op == Env::BK_L0CMP) ? IO_HIGH : ((background_op==Env::BK_DLCMP)?IO_LOW:IO_STOP); //flush stopped
          break;
        default: io_pri = IO_TOTAL;
          break;
      }
    break;
    case CC_MT:
      switch (current_cushion) {
        case CUSHION_NORMAL: io_pri = (background_op == Env::BK_FLUSH)? IO_HIGH: IO_LOW;
        break;
        case CUSHION_L0: io_pri = (background_op == Env::BK_FLUSH)? IO_STOP : ((background_op==Env::BK_L0CMP)?IO_HIGH:IO_LOW); //flush stopped
        break;
        case CUSHION_EC: io_pri = (background_op == Env::BK_L0CMP) ? IO_LOW : ((background_op==Env::BK_DLCMP)?IO_HIGH:IO_STOP); //flush stopped
        break;
        case CUSHION_ECL0: io_pri = (background_op == Env::BK_L0CMP) ? IO_HIGH : ((background_op==Env::BK_DLCMP)?IO_LOW:IO_STOP); //flush stopped
        break;
        default: io_pri = IO_TOTAL;
        break;
      }
    break;
    case CC_L0:
      switch (current_cushion) {
        case CUSHION_NORMAL: io_pri = (background_op == Env::BK_L0CMP)? IO_HIGH: ((background_op == Env::BK_FLUSH)?IO_STOP:IO_LOW); // flush stopped;
        break;
        case CUSHION_L0: io_pri = (background_op == Env::BK_FLUSH)? IO_STOP : ((background_op==Env::BK_L0CMP)?IO_HIGH:IO_LOW); //flush stopped
        break;
        case CUSHION_EC: io_pri = (background_op == Env::BK_L0CMP) ? IO_LOW : ((background_op==Env::BK_DLCMP)?IO_HIGH:IO_STOP); //flush stopped
        break;
        case CUSHION_ECL0: io_pri = (background_op == Env::BK_L0CMP) ? IO_HIGH : ((background_op==Env::BK_DLCMP)?IO_LOW:IO_STOP); //flush stopped
        break;
        default: io_pri = IO_TOTAL;
        break;
      }
    break;
    case CC_L0MT:
      switch (current_cushion) {
        case CUSHION_NORMAL: io_pri = (background_op == Env::BK_L0CMP)? IO_HIGH: ((background_op == Env::BK_DLCMP)? IO_LOW : IO_STOP); //flush stopped
        break;
        case CUSHION_L0: io_pri = (background_op == Env::BK_FLUSH)? IO_STOP : ((background_op==Env::BK_L0CMP)?IO_HIGH:IO_LOW); //flush stopped
        break;
        case CUSHION_EC: io_pri = (background_op == Env::BK_L0CMP) ? IO_LOW : ((background_op==Env::BK_DLCMP)?IO_HIGH:IO_STOP); //flush stopped
        break;
        case CUSHION_ECL0: io_pri = (background_op == Env::BK_L0CMP) ? IO_HIGH : ((background_op==Env::BK_DLCMP)?IO_LOW:IO_STOP); //flush stopped
        break;
        default: io_pri = IO_TOTAL;
        break;
      }
    break;
    case CC_EC:
      switch (current_cushion) {
        case CUSHION_NORMAL: io_pri = (background_op == Env::BK_DLCMP)? IO_HIGH: (background_op==Env::BK_FLUSH) ? IO_STOP:IO_LOW ; // flush stopped;
        break;
        case CUSHION_L0: io_pri = (background_op == Env::BK_FLUSH)? IO_STOP : ((background_op==Env::BK_L0CMP)?IO_HIGH:IO_LOW); //flush stopped
        break;
        case CUSHION_EC: io_pri = (background_op == Env::BK_L0CMP) ? IO_LOW : ((background_op==Env::BK_DLCMP)?IO_HIGH:IO_STOP); //flush stopped
        break;
        case CUSHION_ECL0: io_pri = (background_op == Env::BK_L0CMP) ? IO_HIGH : ((background_op==Env::BK_DLCMP)?IO_LOW:IO_STOP); //flush stopped
        break;
        default: io_pri = IO_TOTAL;
        break;
      }
    break;
    case CC_ECMT:
      switch (current_cushion) {
        case CUSHION_NORMAL: io_pri = (background_op == Env::BK_L0CMP)? IO_LOW : ((background_op==Env::BK_FLUSH) ? IO_STOP : IO_HIGH) ; //flush stopped
        break;
        case CUSHION_L0: io_pri = (background_op == Env::BK_FLUSH)? IO_STOP : ((background_op==Env::BK_L0CMP)?IO_HIGH:IO_LOW); //flush stopped
        break;
        case CUSHION_EC: io_pri = (background_op == Env::BK_L0CMP) ? IO_LOW : ((background_op==Env::BK_DLCMP)?IO_HIGH:IO_STOP); //flush stopped
        break;
        case CUSHION_ECL0: io_pri = (background_op == Env::BK_L0CMP) ? IO_HIGH : ((background_op==Env::BK_DLCMP)?IO_LOW:IO_STOP); //flush stopped
        break;
        default: io_pri = IO_TOTAL;
        break;
      }
    break;
    case CC_ECL0: io_pri = (background_op == Env::BK_L0CMP)? IO_HIGH: (background_op==Env::BK_FLUSH)?IO_STOP:IO_LOW; // Flush stopped;
    break;
    case CC_ECL0MT: io_pri = (background_op == Env::BK_L0CMP)? IO_HIGH: ((background_op==Env::BK_FLUSH)?IO_STOP:IO_LOW); //flush stopped
    break;
    default: io_pri = IO_TOTAL;
    break;
  }

  return io_pri;
}

void InputRateController::DecideIfNeedRequestAndReturnToken(ColumnFamilyData* cfd,
                                                            Env::BackgroundOp background_op,
                                                            const MutableCFOptions& mutable_cf_options,
                                                            bool& need_request_token, bool& need_return_token) {
  MutexLock g(&request_mutex_);
  BackgroundOp_Priority io_pri = DecideBackgroundOpPriority(
      background_op, cfd->GetName());
  need_request_token = (io_pri != IO_TOTAL);
  need_return_token = (io_pri == IO_HIGH);

  if(need_debug_info_){
    ROCKS_LOG_INFO(cfd->ioptions()->logger,"[%s] DecideIfNeedRequestReturnToken: background_op: %s "
                                           "io_pri: %s "
                                           "cc_cur: %s "
                                           "cc_cushion: %s "
                                           "need_request_token: %s "
                                           "need_return_token: %s ",cfd->GetName().c_str(),
                                           BackgroundOpString(background_op).c_str(),
                                           BackgroundOpPriorityString(io_pri).c_str(),
                                           CCConditionString(cur_cc[cfd->GetName()]).c_str(),
                                           CushionString(cushion[cfd->GetName()]).c_str(),
                                           need_request_token?"true":"false",
                                           need_return_token?"true":"false");
  }
}

size_t InputRateController::RequestToken(size_t bytes, size_t alignment,
                                         ColumnFamilyData* cfd,
                                         Env::BackgroundOp background_op,
                                         const MutableCFOptions& mutable_cf_options) {
  if (alignment > 0) {
    bytes = std::max(alignment, TruncateToPageBoundary(alignment, bytes));
  }
  Request(bytes,cfd,background_op,mutable_cf_options);
  return bytes;
}

void InputRateController::Request(size_t bytes, ColumnFamilyData* cfd,
                                  Env::BackgroundOp background_op,
                                  const MutableCFOptions& mutable_cf_options) {
  MutexLock g(&request_mutex_);
  std::string cfd_name = cfd->GetName();
  InputRateController::BackgroundOp_Priority io_pri = DecideBackgroundOpPriority(background_op,cfd_name);
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
                                           BackgroundOpPriorityString(io_pri).c_str(),
                                           bytes,
                                           CCConditionString(current_cc).c_str(),
                                           CushionString(current_cushion).c_str());
  }

  if(io_pri==IO_TOTAL) {
    return;
  }

  if(need_debug_info_){
    ROCKS_LOG_INFO(cfd->ioptions()->logger,"[%s] RequestToken-GotMutex: "
                                           "background_op: %s "
                                           "io_pri: %s "
                                           "bytes: %zu "
                                           "cc_cur: %s "
                                           "cc_cushion: %s ",cfd->GetName().c_str(),
                                           BackgroundOpString(background_op).c_str(),
                                           BackgroundOpPriorityString(io_pri).c_str(), bytes,
                                           CCConditionString(current_cc).c_str(),
                                           CushionString(current_cushion).c_str());
  }

//  SignalStopOpExcept(cfd,stopped_op,background_op,io_pri);

if(stopped_op.find(cfd_name) != stopped_op.end() &&
          background_op == stopped_op[cfd->GetName()]){
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
                                             BackgroundOpPriorityString(io_pri).c_str(),
                                             bytes,
                                             CCConditionString(current_cc).c_str(),
                                             CushionString(current_cushion).c_str(),
                                             &r);
    }

    do{
      if(background_op != stopped_op[cfd->GetName()]){
        r.signaled_reason = TSREASON_CC_CHANGE;
        break;
      }
      int64_t wait_until = clock_->NowMicros() + low_bkop_max_wait_us;
      r.cv.TimedWait(wait_until);
    }while(true);

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
                                             BackgroundOpPriorityString(io_pri).c_str(),
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
  }else if(io_pri == IO_HIGH){
    ++cur_high_;
//    SignalLowOpShouldBeHighOpNow(cfd,background_op);
  }else if(io_pri == IO_LOW){
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
                                             BackgroundOpPriorityString(io_pri).c_str(),
                                             bytes,
                                             CCConditionString(current_cc).c_str(),
                                             CushionString(current_cushion).c_str(),
                                             &r);
    }

//    if(background_op==Env::BK_DLCMP){
//      do{
//        if(cur_high_.load(std::memory_order_relaxed)==0){
//          break;
//        }
//        int64_t wait_until = clock_->NowMicros() + low_dlcmp_max_wait_us;
//        timeout_occurred = r.cv.TimedWait(wait_until);
//      }while(!timeout_occurred);
//
//    }else{
//      do{
//        if(cur_high_.load(std::memory_order_relaxed)==0){
//          break;
//        }
//        int64_t wait_until = clock_->NowMicros() + low_bkop_max_wait_us;
//        timeout_occurred = r.cv.TimedWait(wait_until);
//      }while(!timeout_occurred);
//    }

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
                                             BackgroundOpPriorityString(io_pri).c_str(),
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
                                             BackgroundOpPriorityString(io_pri).c_str(),
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
                                           BackgroundOpPriorityString(io_pri).c_str(),
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

//void InputRateController::SignalLowOpShouldBeHighOpNow(ColumnFamilyData* cfd, Env::BackgroundOp background_op) {
//  request_mutex_.AssertHeld();
//  std::deque<Req*> queue;
//  for(auto& r: low_bkop_queue_[cfd->GetName()]){
//    if(r->background_op == background_op){
//      queue.push_back(r);
//    }
//  }
//  int sum = queue.size();
//  int cnt = 0;
//  for(auto& r : queue){
//    cnt++;
//    if(need_debug_info_){
//      ROCKS_LOG_INFO(cfd->ioptions()->logger,"[%s] RequestToken-StartSignal-LOWop: "
//                                             "background_op: %s "
//                                             "io_pri: HIGH "
//                                             "sum: %d "
//                                             "count %d "
//                                             "req: %p "
//                                             "signal_reason: TSREASON_CC_CHANGE "
//                                             "signaled_op: %s", cfd->GetName().c_str(),
//                                             BackgroundOpString(background_op).c_str(),
//                                             sum,
//                                             cnt,
//                                             r,
//                                             BackgroundOpString(r->background_op).c_str());
//    }
//
//    r->signaled_reason = TSREASON_CC_CHANGE;
//    r->cv.Signal();
//    if(need_debug_info_){
//      ROCKS_LOG_INFO(cfd->ioptions()->logger,"[%s] RequestToken-FinishSignal-LOWop: "
//                                             "background_op: %s "
//                                             "io_pri: HIGH "
//                                             "sum: %d "
//                                             "count %d "
//                                             "req: %p "
//                                             "signal_reason: TSREASON_CC_CHANGE "
//                                             "signaled_op: %s", cfd->GetName().c_str(),
//                                             BackgroundOpString(background_op).c_str(),
//                                             sum,
//                                             cnt,
//                                             r,
//                                             BackgroundOpString(r->background_op).c_str());
//    }
//
//  }
//}

//void InputRateController::SignalStopOpExcept(ColumnFamilyData* cfd, Env::BackgroundOp except_op, Env::BackgroundOp cur_op, BackgroundOp_Priority io_pri) {
//  request_mutex_.AssertHeld();
//  for (int i = Env::BK_TOTAL - 1; i >= Env::BK_FLUSH; --i) {
//    if(i==except_op){
//      continue;
//    }
//
//    int sum = stopped_bkop_queue_[i].size();
//    int cnt = 0;
//    while (!stopped_bkop_queue_[i].empty()) {
//      cnt++;
//      auto r = stopped_bkop_queue_[i].front();
//      stopped_bkop_queue_[i].front()->signaled_by_op = cur_op;
//      stopped_bkop_queue_[i].front()->signaled_reason = TSREASON_CC_CHANGE;
//
//      if(need_debug_info_){
//        ROCKS_LOG_INFO(cfd->ioptions()->logger,"[%s] RequestToken-StartSignal-STOPop: "
//                                               "background_op: %s "
//                                               "io_pri: %s "
//                                               "sum: %d "
//                                               "count %d "
//                                               "req: %p signal_reason: TSREASON_CC_CHANGE "
//                                               "signaled_op: %s ", cfd->GetName().c_str(),
//                                               BackgroundOpString(cur_op).c_str(),
//                                               BackgroundOpPriorityString(io_pri).c_str(),
//                                               sum,
//                                               cnt,
//                                               r,
//                                               BackgroundOpString((Env::BackgroundOp)i).c_str());
//      }
//
//      stopped_bkop_queue_[i].front()->cv.Signal();
//      stopped_bkop_queue_[i].pop_front();
//
//      if(need_debug_info_){
//        ROCKS_LOG_INFO(cfd->ioptions()->logger,"[%s] RequestToken-FinishSignal-STOPop: "
//                                               "background_op: %s "
//                                               "io_pri: %s "
//                                               "sum: %d "
//                                               "count %d "
//                                               "req: %p signal_reason: TSREASON_CC_CHANGE "
//                                               "signaled_op: %s ", cfd->GetName().c_str(),
//                                               BackgroundOpString(cur_op).c_str(),
//                                               BackgroundOpPriorityString(io_pri).c_str(),
//                                               sum,
//                                               cnt,
//                                               r,
//                                               BackgroundOpString((Env::BackgroundOp)i).c_str());
//      }
//
//    }
//
//  }
//}

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
  std::string res;
  switch (cu) {
    case CUSHION_NORMAL:
      res = "CUSION_NORMAL"; break;
    case CUSHION_L0:
      res = "CUSION_L0"; break;
    case CUSHION_EC:
      res = "CUSION_EC"; break;
    case CUSHION_ECL0:
      res = "CUSHION_ECL0"; break;
    default: res = "NA"; break;
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