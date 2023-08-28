//
// Created by Haoyu Gong on 2023.
//

#include "input_rate_controller.h"
#include "db/column_family.h"
#include "db/version_set.h"
#include "logging/logging.h"

namespace ROCKSDB_NAMESPACE{
const int64_t low_bkop_max_wait_us = 100000; // 1/10 sec
const int64_t low_dlcmp_max_wait_us = 200000; // 4/10 sec
struct InputRateController::Req {
  explicit Req(int64_t _bytes, port::Mutex* _mu, Env::BackgroundOp _background_op,
               int _blocked_reason, ThreadSignaledReason _signaled_reason, Env::BackgroundOp _signaled_by_op)
  : request_bytes(_bytes),
        bytes(_bytes),
        cv(_mu),
        background_op(_background_op),
        blocked_reason(_blocked_reason),
        signaled_reason(_signaled_reason),
        signaled_by_op(_signaled_by_op){}
  int64_t request_bytes;
  int64_t bytes;
  port::CondVar cv;
  Env::BackgroundOp background_op;
  int blocked_reason;
  ThreadSignaledReason signaled_reason;
  Env::BackgroundOp signaled_by_op; //if req is blocked which op signaled it
};

InputRateController::InputRateController()
: clock_(SystemClock::Default()),
cur_high_(0),
prev_write_stall_condition_(0),
exit_cv_(&request_mutex_),
requests_to_wait_(0),
stop_(false),
compaction_nothing_todo_when_dlcc_(false){}

InputRateController::~InputRateController() {
  MutexLock g(&request_mutex_);
  stop_ = true;
  std::deque<Req*>::size_type queues_size_sum = 0;
  for (int i = Env::BK_FLUSH; i < Env::BK_TOTAL; ++i) {
    queues_size_sum += stopped_bkop_queue_[i].size();
  }
  queues_size_sum += low_bkop_queue_.size();
  requests_to_wait_ = static_cast<int32_t>(queues_size_sum);

  for (int i = Env::BK_TOTAL - 1; i >= Env::BK_FLUSH; --i) {
    std::deque<Req*> queue = stopped_bkop_queue_[i];
    while (!queue.empty()) {
      queue.front()->signaled_reason = TSREASON_SHUTDOWN;
      queue.front()->cv.Signal();
      queue.pop_front();
    }
  }
  for(auto& r : low_bkop_queue_){
    r->cv.Signal();
  }

  while(requests_to_wait_ > 0){
    exit_cv_.Wait();
  }
}

int InputRateController::DecideCurDiskWriteStallCondition(VersionStorageInfo* vstorage, const MutableCFOptions& mutable_cf_options) {
  int result = InputRateController::CCV_NORMAL;
  int num_l0_sst = vstorage->l0_delay_trigger_count();
//  uint64_t estimated_compaction_needed_bytes = vstorage->estimated_compaction_needed_bytes();
  uint64_t estimated_compaction_needed_bytes = vstorage->estimated_compaction_needed_bytes_deeperlevel();

  bool L0 = (num_l0_sst >= mutable_cf_options.level0_stop_writes_trigger);
  bool DL = (estimated_compaction_needed_bytes >= (uint64_t)(mutable_cf_options.hard_pending_compaction_bytes_limit ));
  result = (L0 ? 2 : 0) + (DL ? 4 : 0);
  return result;
}

int InputRateController::DecideCurWriteStallCondition(ColumnFamilyData* cfd,
                                                      const MutableCFOptions& mutable_cf_options){
  int result = InputRateController::CCV_NORMAL;
  int num_unflushed_memtables = cfd->imm()->NumNotFlushed();
  Version* current = cfd->current();
  if(current!= nullptr){
    auto* vstorage = current->storage_info();
    int num_l0_sst = vstorage->l0_delay_trigger_count();
//    uint64_t estimated_compaction_needed_bytes = vstorage->estimated_compaction_needed_bytes();
    uint64_t estimated_compaction_needed_bytes = vstorage->estimated_compaction_needed_bytes_deeperlevel();

    bool MT = (num_unflushed_memtables >= mutable_cf_options.max_write_buffer_number);
    bool L0 = (num_l0_sst >= mutable_cf_options.level0_stop_writes_trigger);
    bool DL = (estimated_compaction_needed_bytes >= (uint64_t)(mutable_cf_options.hard_pending_compaction_bytes_limit ));
    result = (MT ? 1 : 0) + (L0 ? 2 : 0) + (DL ? 4 : 0);
  }
  return result;
}

int InputRateController::DecideWriteStallChange(ColumnFamilyData* cfd, const MutableCFOptions& mutable_cf_options, int ccv_cur) {
  int result = InputRateController::CUSHION_NORMAL;
  bool prev_L0 = (ccv_cur >> 1) & 1;
  bool prev_DL = (ccv_cur >> 2) & 1;
  bool cur_L0 = (ccv_cur >> 1) & 1;
  bool cur_DL = (ccv_cur >> 2) & 1;
  int pre_ccv = prev_write_stall_condition_.load(std::memory_order_relaxed);

  if( pre_ccv == ccv_cur || pre_ccv == CCV_NORMAL){
    return result;
  }
  if(prev_L0 && (!cur_L0)){
    Version* current = cfd->current();
    //if L0-CC was previously violated and now is not
    // we should further check if it leaves room for flush
    assert(current!=nullptr);
    auto* vstorage = current->storage_info();
    int l0_sst_num = vstorage->l0_delay_trigger_count();
    int l0_sst_limit = mutable_cf_options.level0_stop_writes_trigger;
    if(l0_sst_num > (int)(l0_sst_limit*(3/4))){
      result += 1;
    }
  }
  if(prev_DL && (!cur_DL)){
    Version* current = cfd->current();
    //if DL-CC was previously violated and now is not
    // we should further check if it leaves room for L0-L1 cmp
    assert(current!=nullptr);
    auto* vstorage = current->storage_info();
//    uint64_t cmp_bytes_needed = vstorage->estimated_compaction_needed_bytes();
    uint64_t cmp_bytes_needed = vstorage->estimated_compaction_needed_bytes_deeperlevel();
    uint64_t cmp_bytes_limit = mutable_cf_options.hard_pending_compaction_bytes_limit;
    if(cmp_bytes_needed > (uint64_t)(cmp_bytes_limit*(3/4))){
      result += 2;
    }
  }
  return result;
}

InputRateController::BackgroundOp_Priority InputRateController::DecideBackgroundOpPriority(Env::BackgroundOp background_op,
                                                                                           int cur_ccv,int cushion) {
  InputRateController::BackgroundOp_Priority io_pri;
  switch (cur_ccv) {
    case CCV_NORMAL:
      switch (cushion) {
        case CUSHION_NORMAL: io_pri = (background_op == Env::BK_DLCMP)? IO_LOW: ((background_op == Env::BK_L0CMP)?IO_HIGH:IO_TOTAL);
          break;
        case CUSHION_L0: io_pri = (background_op == Env::BK_FLUSH) ? IO_STOP : ((background_op==Env::BK_L0CMP)?IO_HIGH:IO_LOW);
          break;
        case CUSHION_DL: io_pri = (background_op == Env::BK_L0CMP) ?
                                                    (compaction_nothing_todo_when_dlcc_.load(std::memory_order_relaxed)?IO_HIGH:IO_STOP) :
                                                    ((background_op==Env::BK_DLCMP)?IO_HIGH:IO_LOW);
          break;
        case CUSHION_DLL0: io_pri = (background_op == Env::BK_L0CMP) ?
                                                    (compaction_nothing_todo_when_dlcc_.load(std::memory_order_relaxed)?IO_HIGH:IO_STOP) :
                                                    ((background_op==Env::BK_DLCMP)?IO_HIGH:IO_LOW);
          break;
        default: io_pri = IO_TOTAL;
          break;
      }
    break;
    case CCV_MT: io_pri = (background_op == Env::BK_FLUSH)? IO_HIGH: IO_LOW; // no background op stopped; Flush HIGH; other LOW
    break;
    case CCV_L0: io_pri = (background_op == Env::BK_L0CMP)? IO_HIGH: ((background_op == Env::BK_FLUSH)?IO_STOP:IO_LOW); // Flush stopped;
    break;
    case CCV_L0MT: io_pri = (background_op == Env::BK_L0CMP)? IO_HIGH: ((background_op == Env::BK_FLUSH) ? IO_STOP : IO_LOW);  //Flush stopped;
    break;
    case CCV_DL: io_pri = (background_op == Env::BK_DLCMP)? IO_HIGH:
                                                ((background_op == Env::BK_L0CMP)?
                                                (compaction_nothing_todo_when_dlcc_.load(std::memory_order_relaxed)?IO_HIGH:IO_STOP) : IO_LOW); // L0-L1 compaction stopped;
    break;
//    case CCV_DLMT: io_pri = (background_op == Env::BK_DLCMP)? IO_HIGH:
//                                                ((background_op == Env::BK_L0CMP)?
//                                                (compaction_nothing_todo_when_dlcc_.load(std::memory_order_relaxed)?IO_LOW:IO_STOP):IO_LOW); // L0-L1 compaction stopped;
      case CCV_DLMT: io_pri = (background_op == Env::BK_FLUSH)? IO_HIGH:
                                                  ((background_op == Env::BK_L0CMP)?
                                                  (compaction_nothing_todo_when_dlcc_.load(std::memory_order_relaxed)?IO_LOW:IO_STOP):IO_LOW); // L0-L1 compaction stopped;
    break;
    case CCV_DLL0: io_pri = (background_op == Env::BK_DLCMP)? IO_HIGH:
                                                ((background_op==Env::BK_L0CMP)?
                                                (compaction_nothing_todo_when_dlcc_.load(std::memory_order_relaxed)?IO_HIGH:IO_LOW):IO_STOP); // Flush stopped;
    break;
    case CCV_DLL0MT: io_pri = (background_op == Env::BK_DLCMP)? IO_HIGH: IO_LOW;
    break;
    default: io_pri = IO_TOTAL;
    break;
  }

  return io_pri;
}

Env::BackgroundOp InputRateController::DecideStoppedBackgroundOp(int cur_ccv,int cushion) {
  Env::BackgroundOp stopped_op;
  switch (cur_ccv) {
    case CCV_NORMAL:
      switch (cushion) {
        case CUSHION_NORMAL: stopped_op = Env::BK_TOTAL;
        break;
        case CUSHION_L0: stopped_op = Env::BK_FLUSH;
        break;
        case CUSHION_DL: stopped_op = compaction_nothing_todo_when_dlcc_.load(std::memory_order_relaxed)? Env::BK_TOTAL : Env::BK_L0CMP;
        break;
        case CUSHION_DLL0: stopped_op = compaction_nothing_todo_when_dlcc_.load(std::memory_order_relaxed)? Env::BK_TOTAL : Env::BK_L0CMP;
        break;
        default: stopped_op = Env::BK_TOTAL;
        break;
      }
    break;
    case CCV_MT: stopped_op = Env::BK_TOTAL;
    break;
    case CCV_L0: stopped_op = Env::BK_FLUSH;
    break;
    case CCV_L0MT: stopped_op = Env::BK_FLUSH;
    break;
    case CCV_DL: stopped_op = compaction_nothing_todo_when_dlcc_.load(std::memory_order_relaxed)? Env::BK_TOTAL : Env::BK_L0CMP;
    break;
    case CCV_DLMT: stopped_op = compaction_nothing_todo_when_dlcc_.load(std::memory_order_relaxed)? Env::BK_TOTAL : Env::BK_L0CMP;
    break;
    case CCV_DLL0: stopped_op = Env::BK_FLUSH;
    break;
    case CCV_DLL0MT: stopped_op = Env::BK_TOTAL;
    break;
    default: stopped_op = Env::BK_TOTAL;
    break;
  }
  return stopped_op;
}

void InputRateController::DecideIfNeedRequestReturnToken(ColumnFamilyData* cfd,Env::BackgroundOp background_op, const MutableCFOptions& mutable_cf_options, bool& need_request_token, bool& need_return_token) {
  int ccv_cur = DecideCurWriteStallCondition(cfd,mutable_cf_options);
  int cushion = DecideWriteStallChange(cfd,mutable_cf_options,ccv_cur);
  BackgroundOp_Priority io_pri = DecideBackgroundOpPriority(
      background_op, ccv_cur,cushion);
  need_request_token = (io_pri != IO_TOTAL);
  need_return_token = (io_pri == IO_HIGH);

  ROCKS_LOG_INFO(cfd->ioptions()->logger,"[%s] DecideIfNeedRequestReturnToken: background_op: %s "
                 "io_pri: %s "
                 "ccv_cur: %s "
                 "ccv_cushion: %s "
                 "need_request_token: %s "
                 "need_return_token: %s "
                 "compaction_nothing_todo_when_dlccv: %s ",cfd->GetName().c_str(),
                 BackgroundOpString(background_op).c_str(),
                 BackgroundOpPriorityString(io_pri).c_str(),
                 CCVConditionString(ccv_cur).c_str(),
                 CushionString(cushion).c_str(),
                 need_request_token?"true":"false",
                 need_return_token?"true":"false",
                 compaction_nothing_todo_when_dlcc_.load(std::memory_order_relaxed)?"true":"false");
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
  int ccv_cur = DecideCurWriteStallCondition(cfd,mutable_cf_options);
  int cushion = DecideWriteStallChange(cfd,mutable_cf_options,ccv_cur);
  InputRateController::BackgroundOp_Priority io_pri = DecideBackgroundOpPriority(background_op,ccv_cur,cushion);
  ROCKS_LOG_INFO(cfd->ioptions()->logger,"[%s] RequestToken-Enter: "
                                         "background_op: %s "
                                         "io_pri: %s "
                                         "bytes: %zu "
                                         "ccv_cur: %s "
                                         "ccv_cushion: %s ",cfd->GetName().c_str(),
                                         BackgroundOpString(background_op).c_str(),
                                         BackgroundOpPriorityString(io_pri).c_str(),
                                         bytes,
                                         CCVConditionString(ccv_cur).c_str(),
                                         CushionString(cushion).c_str());
  if(io_pri==IO_TOTAL) {
    return;
  }
  Env::BackgroundOp stopped_op = DecideStoppedBackgroundOp(ccv_cur,cushion);

  MutexLock g(&request_mutex_);
  ROCKS_LOG_INFO(cfd->ioptions()->logger,"[%s] RequestToken-GotMutex: "
                                         "background_op: %s "
                                         "io_pri: %s "
                                         "bytes: %zu "
                                         "ccv_cur: %s "
                                         "ccv_cushion: %s ",cfd->GetName().c_str(),
                                         BackgroundOpString(background_op).c_str(),
                                         BackgroundOpPriorityString(io_pri).c_str(), bytes,
                                         CCVConditionString(ccv_cur).c_str(),
                                         CushionString(cushion).c_str());

  int pre_ccv = prev_write_stall_condition_.load(std::memory_order_relaxed);

  if(cushion==CUSHION_NORMAL && pre_ccv != ccv_cur){
    ROCKS_LOG_INFO(cfd->ioptions()->logger,"[%s] RequestToken-StartUpdatePrevCCVCondition: "
                                           "from: %s "
                                           "to: %s ", cfd->GetName().c_str(),
                                           CCVConditionString(pre_ccv).c_str(),
                                           CCVConditionString(ccv_cur).c_str());
    UpdatePrevCCVCondition(pre_ccv,ccv_cur);
    ROCKS_LOG_INFO(cfd->ioptions()->logger,"[%s] RequestToken-FinishUpdatePrevCCVCondition: "
                                           "ccv_prev: %s ", cfd->GetName().c_str(),
                                           CCVConditionString(prev_write_stall_condition_.load(std::memory_order_relaxed)).c_str());
  }

  SignalStopOpExcept(cfd,stopped_op,background_op,io_pri);

  if(background_op == stopped_op){
    Req r(bytes, &request_mutex_, background_op,ccv_cur, TSREASON_TOTAL, Env::BK_NONE);
    stopped_bkop_queue_[stopped_op].push_back(&r);
    ROCKS_LOG_INFO(cfd->ioptions()->logger,"[%s] RequestToken-StartWait: "
                                           "background_op: %s "
                                           "io_pri: %s "
                                           "bytes: %zu "
                                           "ccv_cur: %s "
                                           "ccv_cushion: %s "
                                           "req: %p ", cfd->GetName().c_str(),
                                           BackgroundOpString(background_op).c_str(),
                                           BackgroundOpPriorityString(io_pri).c_str(),
                                           bytes,
                                           CCVConditionString(ccv_cur).c_str(),
                                           CushionString(cushion).c_str(),
                                           &r);
    if(!stop_){
      r.cv.Wait();
    }
    ROCKS_LOG_INFO(cfd->ioptions()->logger,"[%s] RequestToken-FinishWait-Signaled: "
                                           "backgroundop: %s "
                                           "io_pri: %s "
                                           "bytes: %zu "
                                           "ccv_when_req_issue: %s "
                                           "ccv_when_req_signaled: %s "
                                           "req: %p "
                                           "signaled_reason: %s "
                                           "signaled_by_op: %s ", cfd->GetName().c_str(),
                                           BackgroundOpString(background_op).c_str(),
                                           BackgroundOpPriorityString(io_pri).c_str(),
                                           bytes,
                                           CCVConditionString(ccv_cur).c_str(),
                                           CCVConditionString(prev_write_stall_condition_.load(std::memory_order_relaxed)).c_str(),
                                           &r,
                                           TSReasonString(r.signaled_reason).c_str(),
                                           BackgroundOpString(r.signaled_by_op).c_str());
    if(r.signaled_reason==TSREASON_NOCMP_DLCC || (r.signaled_reason==TSREASON_TOTAL)){
      stopped_bkop_queue_[background_op].erase(std::remove(stopped_bkop_queue_[background_op].begin(), stopped_bkop_queue_[background_op].end(), &r), stopped_bkop_queue_[background_op].end());
    }

  }else if(io_pri == IO_HIGH){
    ++cur_high_;
    SignalLowOpShouldBeHighOpNow(cfd,background_op);
  }else if(io_pri == IO_LOW){
    Req r(bytes, &request_mutex_, background_op,ccv_cur,TSREASON_TOTAL, Env::BK_NONE);
    low_bkop_queue_.push_back(&r);
    bool timeout_occurred = false;
    ROCKS_LOG_INFO(cfd->ioptions()->logger,"[%s] RequestToken-StartWait: "
                                           "background_op: %s "
                                           "io_pri: %s "
                                           "bytes: %zu "
                                           "ccv_cur: %s "
                                           "ccv_cushion: %s "
                                           "req: %p", cfd->GetName().c_str(),
                                           BackgroundOpString(background_op).c_str(),
                                           BackgroundOpPriorityString(io_pri).c_str(),
                                           bytes,
                                           CCVConditionString(ccv_cur).c_str(),
                                           CushionString(cushion).c_str(),
                                           &r);
    if(background_op==Env::BK_DLCMP){
      do{
        if(cur_high_.load(std::memory_order_relaxed)==0){
          break;
        }
        int64_t wait_until = clock_->NowMicros() + low_dlcmp_max_wait_us;
        timeout_occurred = r.cv.TimedWait(wait_until);
      }while(!timeout_occurred);

    }else{
      do{
        if(cur_high_.load(std::memory_order_relaxed)==0){
          break;
        }
        int64_t wait_until = clock_->NowMicros() + low_bkop_max_wait_us;
        timeout_occurred = r.cv.TimedWait(wait_until);
      }while(!timeout_occurred);
    }

    if(timeout_occurred){
      r.signaled_reason = TSREASON_TIMEOUT;
      ROCKS_LOG_INFO(cfd->ioptions()->logger,"[%s] RequestToken-FinishWait-Signaled: "
                                             "backgroundop: %s "
                                             "io_pri: %s "
                                             "bytes: %zu "
                                             "ccv_when_req_issue: %s "
                                             "ccv_when_req_signaled: %s "
                                             "req: %p "
                                             "signaled_reason: %s "
                                             "signaled_by_op: %s ", cfd->GetName().c_str(),
                                             BackgroundOpString(background_op).c_str(),
                                             BackgroundOpPriorityString(io_pri).c_str(),
                                             bytes,
                                             CCVConditionString(ccv_cur).c_str(),
                                             CCVConditionString(prev_write_stall_condition_.load(std::memory_order_relaxed)).c_str(),
                                             &r,
                                             TSReasonString(r.signaled_reason).c_str(),
                                             BackgroundOpString(r.signaled_by_op).c_str());
    }else if(r.signaled_by_op==Env::BK_NONE){
      r.signaled_reason = TSREASON_ZERO_HIGH;
      ROCKS_LOG_INFO(cfd->ioptions()->logger,"[%s] RequestToken-FinishWait-Signaled: "
                                             "backgroundop: %s "
                                             "io_pri: %s "
                                             "bytes: %zu "
                                             "ccv_when_req_issue: %s "
                                             "ccv_when_req_signaled: %s "
                                             "req: %p "
                                             "signaled_reason: %s "
                                             "signaled_by_op: %s ", cfd->GetName().c_str(),
                                             BackgroundOpString(background_op).c_str(),
                                             BackgroundOpPriorityString(io_pri).c_str(),
                                             bytes,
                                             CCVConditionString(ccv_cur).c_str(),
                                             CCVConditionString(prev_write_stall_condition_.load(std::memory_order_relaxed)).c_str(),
                                             &r,
                                             TSReasonString(r.signaled_reason).c_str(),
                                             BackgroundOpString(r.signaled_by_op).c_str());
    }else{
      ROCKS_LOG_INFO(cfd->ioptions()->logger,"[%s] RequestToken-FinishWait-Signaled: "
                                             "backgroundop: %s "
                                             "io_pri: %s "
                                             "bytes: %zu "
                                             "ccv_when_req_issue: %s "
                                             "ccv_when_req_signaled: %s "
                                             "req: %p "
                                             "signaled_reason: %s "
                                             "signaled_by_op: %s ", cfd->GetName().c_str(),
                                             BackgroundOpString(background_op).c_str(),
                                             BackgroundOpPriorityString(io_pri).c_str(),
                                             bytes,
                                             CCVConditionString(ccv_cur).c_str(),
                                             CCVConditionString(prev_write_stall_condition_.load(std::memory_order_relaxed)).c_str(),
                                             &r,
                                             TSReasonString(r.signaled_reason).c_str(),
                                             BackgroundOpString(r.signaled_by_op).c_str());
    }

    // When LOW thread is signaled, it should be removed from low_bkop_queue_ by itself
    // Make sure don't iterate low_bkop_queue_ from another thread while trying to signal LOW threads in low_bkop_queue_
    low_bkop_queue_.erase(std::remove(low_bkop_queue_.begin(), low_bkop_queue_.end(), &r), low_bkop_queue_.end());
  }

  if(stop_){
    --requests_to_wait_;
    ROCKS_LOG_INFO(cfd->ioptions()->logger,"[%s] RequestToken-Decrease-requests_to_wait: "
                                           "background_op: %s "
                                           "io_pri: %s "
                                           "bytes: %zu "
                                           "ccv_when_req_issue: %s "
                                           "cushion_when_req_issue: %s "
                                           "requests_to_wait: %d ",cfd->GetName().c_str(),
                                           BackgroundOpString(background_op).c_str(),
                                           BackgroundOpPriorityString(io_pri).c_str(),
                                           bytes,
                                           CCVConditionString(ccv_cur).c_str(),
                                           CushionString(cushion).c_str(),
                                           requests_to_wait_);
    exit_cv_.Signal();
  }
  ROCKS_LOG_INFO(cfd->ioptions()->logger,"[%s] RequestToken-ReleaseMutex: "
                                         "background_op: %s "
                                         "io_pri: %s "
                                         "bytes: %zu "
                                         "ccv_when_req_issue: %s "
                                         "cushion_when_req_issue: %s ",cfd->GetName().c_str(),
                                         BackgroundOpString(background_op).c_str(),
                                         BackgroundOpPriorityString(io_pri).c_str(),
                                         bytes,
                                         CCVConditionString(ccv_cur).c_str(),
                                         CushionString(cushion).c_str());

}

void InputRateController::ReturnToken(ColumnFamilyData* cfd, Env::BackgroundOp background_op) {
  ROCKS_LOG_INFO(cfd->ioptions()->logger,"[%s] ReturnToken-Enter: "
                 "background_op: %s "
                 "io_pri: HIGH ", cfd->GetName().c_str(),
                 BackgroundOpString(background_op).c_str());
  MutexLock g(&request_mutex_);
  ROCKS_LOG_INFO(cfd->ioptions()->logger,"[%s] ReturnToken-GotMutex: "
                 "background_op: %s "
                 "io_pri: HIGH ", cfd->GetName().c_str(),
                 BackgroundOpString(background_op).c_str());
  --cur_high_;
  if(cur_high_.load(std::memory_order_relaxed)==0 && !low_bkop_queue_.empty()){
    Req* r = low_bkop_queue_.front();
    ROCKS_LOG_INFO(cfd->ioptions()->logger,"[%s] ReturnToken-StartSignal-LOWop: "
                                           "background_op: %s "
                                           "io_pri: HIGH "
                                           "req: %p "
                                           "signal_reason: TSREASON_ZERO_HIGH "
                                           "signaled_op: %s", cfd->GetName().c_str(),
                                           BackgroundOpString(background_op).c_str(),
                                           r,
                                           BackgroundOpString(r->background_op).c_str());
    r->signaled_reason = TSREASON_ZERO_HIGH;
    r->signaled_by_op = background_op;
    r->cv.Signal();
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
  ROCKS_LOG_INFO(cfd->ioptions()->logger,"[%s] ReturnToken-ReleaseMutex: "
                 "background_op: %s io_pri: HIGH", cfd->GetName().c_str(),
                 BackgroundOpString(background_op).c_str());
}

void InputRateController::SignalLowOpShouldBeHighOpNow(ColumnFamilyData* cfd, Env::BackgroundOp background_op) {
  request_mutex_.AssertHeld();
  std::deque<Req*> queue;
  for(auto& r: low_bkop_queue_){
    if(r->background_op == background_op){
      queue.push_back(r);
    }
  }
  int sum = queue.size();
  int cnt = 0;
  for(auto& r : queue){
    cnt++;
    ROCKS_LOG_INFO(cfd->ioptions()->logger,"[%s] RequestToken-StartSignal-LOWop: "
                                           "background_op: %s "
                                           "io_pri: HIGH "
                                           "sum: %d "
                                           "count %d "
                                           "req: %p "
                                           "signal_reason: TSREASON_CCV_CHANGE "
                                           "signaled_op: %s", cfd->GetName().c_str(),
                                           BackgroundOpString(background_op).c_str(),
                                           sum,
                                           cnt,
                                           r,
                                           BackgroundOpString(r->background_op).c_str());
    r->signaled_reason = TSREASON_CCV_CHANGE;
    r->cv.Signal();
    ROCKS_LOG_INFO(cfd->ioptions()->logger,"[%s] RequestToken-FinishSignal-LOWop: "
                                           "background_op: %s "
                                           "io_pri: HIGH "
                                           "sum: %d "
                                           "count %d "
                                           "req: %p "
                                           "signal_reason: TSREASON_CCV_CHANGE "
                                           "signaled_op: %s", cfd->GetName().c_str(),
                                           BackgroundOpString(background_op).c_str(),
                                           sum,
                                           cnt,
                                           r,
                                           BackgroundOpString(r->background_op).c_str());
  }
}

void InputRateController::SignalStopOpWhenNoCmpButDLCC(ColumnFamilyData* cfd) {
  MutexLock g(&request_mutex_);
  compaction_nothing_todo_when_dlcc_.store(true);
  for (int i = Env::BK_TOTAL - 1; i >= Env::BK_FLUSH; --i) {
    std::deque<Req*> queue;

    for(auto& r : stopped_bkop_queue_[i]){
      if((r->blocked_reason >> 2) & 1){
        queue.push_back(r);
      }
    }

    int sum = queue.size();
    int cnt = 0;
    while(!queue.empty()){
      cnt++;
      auto r = queue.front();
      queue.front()->signaled_reason = TSREASON_NOCMP_DLCC;
      queue.front()->signaled_by_op = Env::BK_TOTAL;
      ROCKS_LOG_INFO(cfd->ioptions()->logger,"[%s] DBImplCmp-StartSignal-STOPop: "
                                             "sum: %d "
                                             "count %d "
                                             "req: %p signal_reason: TSREASON_NOCMP_DLCC "
                                             "signaled_op: %s ", cfd->GetName().c_str(),
                                             sum,
                                             cnt,
                                             r,
                                             BackgroundOpString((Env::BackgroundOp)i).c_str());
      queue.front()->cv.Signal();
      queue.pop_front();
      ROCKS_LOG_INFO(cfd->ioptions()->logger,"[%s] DBImplCmp-FinishSignal-STOPop: "
                                             "sum: %d "
                                             "count %d "
                                             "req: %p signal_reason: TSREASON_NOCMP_DLCC "
                                             "signaled_op: %s ", cfd->GetName().c_str(),
                                             sum,
                                             cnt,
                                             r,
                                             BackgroundOpString((Env::BackgroundOp)i).c_str());
    }

  }
}

void InputRateController::SignalStopOpExcept(ColumnFamilyData* cfd, Env::BackgroundOp except_op, Env::BackgroundOp cur_op, BackgroundOp_Priority io_pri) {
  request_mutex_.AssertHeld();
  for (int i = Env::BK_TOTAL - 1; i >= Env::BK_FLUSH; --i) {
    if(i==except_op){
      continue;
    }

    int sum = stopped_bkop_queue_[i].size();
    int cnt = 0;
    while (!stopped_bkop_queue_[i].empty()) {
      cnt++;
      auto r = stopped_bkop_queue_[i].front();
      stopped_bkop_queue_[i].front()->signaled_by_op = cur_op;
      stopped_bkop_queue_[i].front()->signaled_reason = TSREASON_CCV_CHANGE;

      ROCKS_LOG_INFO(cfd->ioptions()->logger,"[%s] RequestToken-StartSignal-STOPop: "
                                             "background_op: %s "
                                             "io_pri: %s "
                                             "sum: %d "
                                             "count %d "
                                             "req: %p signal_reason: TSREASON_CCV_CHANGE "
                                             "signaled_op: %s ", cfd->GetName().c_str(),
                     BackgroundOpString(cur_op).c_str(),
                     BackgroundOpPriorityString(io_pri).c_str(),
                                             sum,
                                             cnt,
                                             r,
                                             BackgroundOpString((Env::BackgroundOp)i).c_str());

      stopped_bkop_queue_[i].front()->cv.Signal();
      stopped_bkop_queue_[i].pop_front();

      ROCKS_LOG_INFO(cfd->ioptions()->logger,"[%s] RequestToken-FinishSignal-STOPop: "
                                             "background_op: %s "
                                             "io_pri: %s "
                                             "sum: %d "
                                             "count %d "
                                             "req: %p signal_reason: TSREASON_CCV_CHANGE "
                                             "signaled_op: %s ", cfd->GetName().c_str(),
                                             BackgroundOpString(cur_op).c_str(),
                                             BackgroundOpPriorityString(io_pri).c_str(),
                                             sum,
                                             cnt,
                                             r,
                                             BackgroundOpString((Env::BackgroundOp)i).c_str());
    }

  }
}

void InputRateController::SetCmpNoWhenDLCC(bool nocmp) {
  MutexLock g(&request_mutex_);
  compaction_nothing_todo_when_dlcc_.store(nocmp);
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

std::string InputRateController::CCVConditionString(int ccv) {
  std::string res;
  switch (ccv) {
    case CCV_NORMAL: res = "CC_NORMAL";
      break;
      case CCV_MT: res = "CC_MT";
      break;
      case CCV_L0: res = "CC_L0";
      break;
      case CCV_DL: res = "CC_DL";
      break;
      case CCV_L0MT: res = "CC_L0MT";
      break;
      case CCV_DLMT: res = "CC_DLMT";
      break;
      case CCV_DLL0: res = "CC_DLL0";
      break;
      case CCV_DLL0MT: res = "CC_DLL0MT";
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
    case CUSHION_DL:
      res = "CUSION_DL"; break;
    case CUSHION_DLL0:
      res = "CUSHION_DLL0"; break;
    default: res = "NA"; break;
  }
  return res;
}

std::string InputRateController::TSReasonString(int ts) {
  std::string res;
  switch (ts) {
    case TSREASON_CCV_CHANGE:
      res = "TSREASON_CCV_CHANGE"; break;
    case TSREASON_ZERO_HIGH:
      res = "TSREASON_ZERO_HIGH"; break;
    case TSREASON_TIMEOUT:
      res = "TSREASON_TIMEOUT"; break;
    case TSREASON_NOCMP_DLCC:
      res = "TSREASON_NOCMP_DLCC"; break;
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