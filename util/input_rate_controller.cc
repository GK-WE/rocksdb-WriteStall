//
// Created by Haoyu Gong on 2023.
//

#include "input_rate_controller.h"
#include "db/column_family.h"
#include "db/version_set.h"
#include "logging/logging.h"

namespace ROCKSDB_NAMESPACE{
const int64_t low_bkop_max_wait_us = 100000; // 1/10 sec
const int64_t low_dlcmp_max_wait_us = 400000; // 4/10 sec
struct InputRateController::Req {
  explicit Req(int64_t _bytes, port::Mutex* _mu, Env::BackgroundOp _background_op)
  : request_bytes(_bytes), bytes(_bytes), cv(_mu), background_op(_background_op) {}
  int64_t request_bytes;
  int64_t bytes;
  port::CondVar cv;
  Env::BackgroundOp background_op;
};

InputRateController::InputRateController()
: clock_(SystemClock::Default()),
cur_high_(0),
prev_write_stall_condition_(0),
exit_cv_(&request_mutex_),
requests_to_wait_(0),
stop_(false){}

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

int InputRateController::DecideCurWriteStallCondition(ColumnFamilyData* cfd,
                                                      const MutableCFOptions& mutable_cf_options){
  int result = InputRateController::WS_NORMAL;
  int num_unflushed_memtables = cfd->imm()->NumNotFlushed();
  Version* current = cfd->current();
  if(current!= nullptr){
    auto* vstorage = current->storage_info();
    int num_l0_sst = vstorage->l0_delay_trigger_count();
    uint64_t estimated_compaction_needed_bytes = vstorage->estimated_compaction_needed_bytes();

    bool MT = (num_unflushed_memtables >= mutable_cf_options.max_write_buffer_number);
    bool L0 = (num_l0_sst >= mutable_cf_options.level0_stop_writes_trigger);
    bool DL = (estimated_compaction_needed_bytes >= mutable_cf_options.hard_pending_compaction_bytes_limit);
    result = (MT ? 1 : 0) + (L0 ? 2 : 0) + (DL ? 4 : 0);
  }
  return result;
}

int InputRateController::DecideWriteStallChange(ColumnFamilyData* cfd, const MutableCFOptions& mutable_cf_options, int ws_cur) {
  int result = InputRateController::CUSHION_NORMAL;
  bool prev_L0 = (ws_cur >> 1) & 1;
  bool prev_DL = (ws_cur >> 2) & 1;
  bool cur_L0 = (ws_cur >> 1) & 1;
  bool cur_DL = (ws_cur >> 2) & 1;
  int pre_ws = prev_write_stall_condition_.load(std::memory_order_relaxed);

  if( pre_ws == ws_cur || pre_ws == WS_NORMAL){
    return result;
  }else if(prev_L0 && (!cur_L0)){
    Version* current = cfd->current();
    //if L0-CC was previously violated and now is not
    // we should further check if it leaves room for flush
    assert(current!=nullptr);
    auto* vstorage = current->storage_info();
    int l0_sst_num = vstorage->l0_delay_trigger_count();
    int l0_sst_limit = mutable_cf_options.level0_stop_writes_trigger;
    if(l0_sst_num > (int)((l0_sst_limit*3)/4)){
      result += 1;
    }
  }else if(prev_DL && (!cur_DL)){
    Version* current = cfd->current();
    //if DL-CC was previously violated and now is not
    // we should further check if it leaves room for L0-L1
    assert(current!=nullptr);
    auto* vstorage = current->storage_info();
    uint64_t cmp_bytes_needed = vstorage->estimated_compaction_needed_bytes();
    uint64_t cmp_bytes_limit = mutable_cf_options.hard_pending_compaction_bytes_limit;
    if(cmp_bytes_needed > (uint64_t)((cmp_bytes_limit*3)/4)){
      result += 2;
    }
  }
  return result;
}

InputRateController::BackgroundOp_Priority InputRateController::DecideBackgroundOpPriority(Env::BackgroundOp background_op,
                                                                                           int cur_ws,int cushion) {
  InputRateController::BackgroundOp_Priority io_pri;
  switch (cur_ws) {
    case WS_NORMAL:
      switch (cushion) {
        case CUSHION_NORMAL: io_pri = (background_op == Env::BK_DLCMP)? IO_LOW: ((background_op == Env::BK_L0CMP)?IO_HIGH:IO_TOTAL);
          break;
        case CUSHION_L0: io_pri = (background_op == Env::BK_FLUSH) ? IO_STOP : ((background_op==Env::BK_L0CMP)?IO_HIGH:IO_LOW);
          break;
        case CUSHION_DL: io_pri = (background_op == Env::BK_L0CMP) ? IO_STOP : ((background_op==Env::BK_DLCMP)?IO_HIGH:IO_LOW);
          break;
        case CUSHION_DLL0: io_pri = (background_op == Env::BK_L0CMP) ? IO_STOP : ((background_op==Env::BK_DLCMP)?IO_HIGH:IO_LOW);
          break;
        default: io_pri = IO_TOTAL;
          break;
      }
    break;
    case WS_MT: io_pri = (background_op == Env::BK_FLUSH)? IO_HIGH: IO_LOW; // no background op stopped; Flush HIGH; other LOW
    break;
    case WS_L0: io_pri = (background_op == Env::BK_L0CMP)? IO_HIGH: ((background_op == Env::BK_FLUSH)?IO_STOP:IO_LOW); // Flush stopped;
    break;
    case WS_L0MT: io_pri = (background_op == Env::BK_L0CMP)? IO_HIGH: ((background_op == Env::BK_FLUSH) ? IO_STOP : IO_LOW);  //Flush stopped;
    break;
    case WS_DL: io_pri = (background_op == Env::BK_DLCMP)? IO_HIGH: ((background_op == Env::BK_L0CMP)?IO_STOP:IO_LOW); // L0-L1 compaction stopped;
    break;
    case WS_DLMT: io_pri = (background_op == Env::BK_DLCMP)? IO_HIGH: ((background_op == Env::BK_L0CMP)?IO_STOP:IO_LOW); // L0-L1 compaction stopped;
    break;
    case WS_DLL0: io_pri = (background_op == Env::BK_DLCMP)? IO_HIGH: ((background_op==Env::BK_FLUSH)?IO_STOP:IO_LOW); // Flush stopped;
    break;
    case WS_DLL0MT: io_pri = (background_op == Env::BK_DLCMP)? IO_HIGH: IO_LOW;
    break;
    default: io_pri = IO_TOTAL;
    break;
  }

  return io_pri;
}

Env::BackgroundOp InputRateController::DecideStoppedBackgroundOp(int cur_ws,int cushion) {
  Env::BackgroundOp stopped_op;
  switch (cur_ws) {
    case WS_NORMAL:
      switch (cushion) {
        case CUSHION_NORMAL: stopped_op = Env::BK_TOTAL;
        break;
        case CUSHION_L0: stopped_op = Env::BK_FLUSH;
        break;
        case CUSHION_DL: stopped_op = Env::BK_L0CMP;
        break;
        case CUSHION_DLL0: stopped_op = Env::BK_L0CMP;
        break;
        default: stopped_op = Env::BK_TOTAL;
        break;
      }
    break;
    case WS_MT: stopped_op = Env::BK_TOTAL;
    break;
    case WS_L0: stopped_op = Env::BK_FLUSH;
    break;
    case WS_L0MT: stopped_op = Env::BK_FLUSH;
    break;
    case WS_DL: stopped_op = Env::BK_L0CMP;
    break;
    case WS_DLMT: stopped_op = Env::BK_L0CMP;
    break;
    case WS_DLL0: stopped_op = Env::BK_FLUSH;
    break;
    case WS_DLL0MT: stopped_op = Env::BK_TOTAL;
    break;
    default: stopped_op = Env::BK_TOTAL;
    break;
  }
  return stopped_op;
}

void InputRateController::DecideIfNeedRequestReturnToken(ColumnFamilyData* cfd,Env::BackgroundOp background_op, const MutableCFOptions& mutable_cf_options, bool& need_request_token, bool& need_return_token) {
  int ws_cur = DecideCurWriteStallCondition(cfd,mutable_cf_options);
  int cushion = DecideWriteStallChange(cfd,mutable_cf_options,ws_cur);
  BackgroundOp_Priority io_pri = DecideBackgroundOpPriority(
      background_op, ws_cur,cushion);
  need_request_token = (io_pri != IO_TOTAL);
  need_return_token = (io_pri == IO_HIGH);

  ROCKS_LOG_INFO(cfd->ioptions()->logger,"[%s] DecideIfNeedRequestReturnToken: backgroundop: %s "
                                         "io_pri: %s ws_cur: %s cushion: %s "
                                         "need_request_token: %s need_return_token %s",
                 cfd->GetName().c_str(),
                 BackgroundOpString(background_op).c_str(),
                 BackgroundOpPriorityString(io_pri).c_str(),
                 WSConditionString(ws_cur).c_str(),
                 CushionString(cushion).c_str(),
                 need_request_token?"true":"false",
                 need_return_token?"true":"false");
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

void InputRateController::ReturnToken(ColumnFamilyData* cfd, Env::BackgroundOp background_op) {
  ROCKS_LOG_INFO(cfd->ioptions()->logger,"[%s] InputRateController::ReturnToken: backgroundop: %s io_pri: HIGH", cfd->GetName().c_str(),
                 BackgroundOpString(background_op).c_str());
  --cur_high_;
  MutexLock g(&request_mutex_);
  if(cur_high_.load(std::memory_order_relaxed)==0 && !low_bkop_queue_.empty()){
    Req* r = low_bkop_queue_.front();
    r->cv.Signal();
  }
}

void InputRateController::Request(size_t bytes, ColumnFamilyData* cfd,
                                  Env::BackgroundOp background_op,
                                  const MutableCFOptions& mutable_cf_options) {
  int ws_cur = DecideCurWriteStallCondition(cfd,mutable_cf_options);
  int cushion = DecideWriteStallChange(cfd,mutable_cf_options,ws_cur);
  InputRateController::BackgroundOp_Priority io_pri = DecideBackgroundOpPriority(background_op,ws_cur,cushion);
  ROCKS_LOG_INFO(cfd->ioptions()->logger,"[%s] InputRateController::RequestToken: backgroundop: %s io_pri: %s bytes: %zu ws_when_reqissue: %s cushion_when_reqissue: %s",
                 cfd->GetName().c_str(),
                 BackgroundOpString(background_op).c_str(),
                 BackgroundOpPriorityString(io_pri).c_str(), bytes,
                 WSConditionString(ws_cur).c_str(),
                 CushionString(cushion).c_str());
  if(io_pri==IO_TOTAL) {
    return;
  }

  Env::BackgroundOp stopped_op = DecideStoppedBackgroundOp(ws_cur,cushion);


  MutexLock g(&request_mutex_);

  int pre_ws = prev_write_stall_condition_.load(std::memory_order_relaxed);

  if(cushion==CUSHION_NORMAL && pre_ws != ws_cur){
    ROCKS_LOG_INFO(cfd->ioptions()->logger,"[%s] Start UpdatePrevWSCondition from %s to %s ", cfd->GetName().c_str(),
                   WSConditionString(pre_ws).c_str(), WSConditionString(ws_cur).c_str());
    UpdatePrevWSCondition(pre_ws,ws_cur);
    ROCKS_LOG_INFO(cfd->ioptions()->logger,"[%s] Finish UpdatePrevWSCondition to %s ", cfd->GetName().c_str(),
                   WSConditionString(prev_write_stall_condition_.load(std::memory_order_relaxed)).c_str());
  }

  for (int i = Env::BK_TOTAL - 1; i >= Env::BK_FLUSH; --i) {
    if(i==stopped_op){
      continue;
    }
    bool isqueue_empty = stopped_bkop_queue_[i].empty();
    if(!isqueue_empty){
      ROCKS_LOG_INFO(cfd->ioptions()->logger,"[%s] backgroundop: %s io_pri: %s Start Signal-STOP-op: %s ", cfd->GetName().c_str(),
                     BackgroundOpString(background_op).c_str(),
                     BackgroundOpPriorityString(io_pri).c_str(),
                     BackgroundOpString((Env::BackgroundOp)i).c_str());
    }
    while (!stopped_bkop_queue_[i].empty()) {
      stopped_bkop_queue_[i].front()->cv.Signal();
      stopped_bkop_queue_[i].pop_front();
    }
    if(!isqueue_empty){
      ROCKS_LOG_INFO(cfd->ioptions()->logger,"[%s] backgroundop: %s io_pri: %s Finish Signal-STOP-op: %s ", cfd->GetName().c_str(),
                     BackgroundOpString(background_op).c_str(),
                     BackgroundOpPriorityString(io_pri).c_str(),
                     BackgroundOpString((Env::BackgroundOp)i).c_str());
    }
  }

  ROCKS_LOG_INFO(cfd->ioptions()->logger,"[%s] backgroundop: %s io_pri: %s Finish Signal-STOP-op if any ", cfd->GetName().c_str(),
                 BackgroundOpString(background_op).c_str(),
                 BackgroundOpPriorityString(io_pri).c_str());

  if(background_op == stopped_op){
    Req r(bytes, &request_mutex_, background_op);
    stopped_bkop_queue_[stopped_op].push_back(&r);
    r.cv.Wait();
    ROCKS_LOG_INFO(cfd->ioptions()->logger,"[%s] WSChange-Signaled-STOP backgroundop: %s io_pri: %s bytes: %zu ws_when_reqissue: %s ws_cur: %s", cfd->GetName().c_str(),
                   BackgroundOpString(background_op).c_str(), BackgroundOpPriorityString(io_pri).c_str(), bytes,
                   WSConditionString(ws_cur).c_str(), WSConditionString(prev_write_stall_condition_.load(std::memory_order_relaxed)).c_str());
  }else if(io_pri == IO_HIGH){
    ++cur_high_;
    std::deque<Req*> queue;
    for(auto& r: low_bkop_queue_){
      if(r->background_op == background_op){
        queue.push_back(r);
      }
    }
    if(!queue.empty()){
      ROCKS_LOG_INFO(cfd->ioptions()->logger,"[%s] backgroundop: %s io_pri: %s Start Signal-LOW-op", cfd->GetName().c_str(),
                     BackgroundOpString(background_op).c_str(),
                     BackgroundOpPriorityString(io_pri).c_str());
    }
    for(auto&r : queue){
      r->cv.Signal();
    }
    if(!queue.empty()){
      ROCKS_LOG_INFO(cfd->ioptions()->logger,"[%s] backgroundop: %s io_pri: %s Finish Signal-LOW-op if any ", cfd->GetName().c_str(),
                     BackgroundOpString(background_op).c_str(),
                     BackgroundOpPriorityString(io_pri).c_str());
    }

  }else if(io_pri == IO_LOW){
    Req r(bytes, &request_mutex_, background_op);
    low_bkop_queue_.push_back(&r);
    if(background_op==Env::BK_DLCMP){
      bool timeout_occurred = false;
      while(cur_high_.load(std::memory_order_relaxed)>0 || timeout_occurred){
        int64_t wait_until = clock_->NowMicros() + low_dlcmp_max_wait_us;
        timeout_occurred = r.cv.TimedWait(wait_until);
      }
      ROCKS_LOG_INFO(cfd->ioptions()->logger,"[%s] NO-HIGH-Signaled-LOW backgroundop: %s io_pri: %s bytes: %zu ws_when_reqissue: %s ws_cur: %s", cfd->GetName().c_str(),
                     BackgroundOpString(background_op).c_str(), BackgroundOpPriorityString(io_pri).c_str(), bytes,
                     WSConditionString(ws_cur).c_str(), WSConditionString(prev_write_stall_condition_.load(std::memory_order_relaxed)).c_str());
    }else{
      bool timeout_occurred = false;
      while(cur_high_.load(std::memory_order_relaxed)>0 || timeout_occurred){
        int64_t wait_until = clock_->NowMicros() + low_bkop_max_wait_us;
        timeout_occurred = r.cv.TimedWait(wait_until);
      }
      ROCKS_LOG_INFO(cfd->ioptions()->logger,"[%s] TIMEOUT-Signaled-LOW backgroundop: %s io_pri: %s bytes: %zu ws_when_reqissue: %s ws_cur: %s cur_high: %d", cfd->GetName().c_str(),
                     BackgroundOpString(background_op).c_str(), BackgroundOpPriorityString(io_pri).c_str(), bytes,
                     WSConditionString(ws_cur).c_str(), WSConditionString(prev_write_stall_condition_.load(std::memory_order_relaxed)).c_str(), cur_high_.load(std::memory_order_relaxed));
    }

    // When LOW thread is signaled, it should be removed from low_bkop_queue_ by itself
    // Make sure don't iterate low_bkop_queue_ from another thread while trying to signal LOW threads in low_bkop_queue_
    low_bkop_queue_.erase(std::remove(low_bkop_queue_.begin(), low_bkop_queue_.end(), &r), low_bkop_queue_.end());
  }

  if(stop_){
    --requests_to_wait_;
    exit_cv_.Signal();
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

std::string InputRateController::WSConditionString(int ws) {
  std::string res;
  switch (ws) {
    case WS_NORMAL: res = "WS_NORMAL";
      break;
      case WS_MT: res = "WS_MT";
      break;
      case WS_L0: res = "WS_L0";
      break;
      case WS_DL: res = "WS_DL";
      break;
      case WS_L0MT: res = "WS_L0MT";
      break;
      case WS_DLMT: res = "WS_DLMT";
      break;
      case WS_DLL0: res = "WS_DLL0";
      break;
      case WS_DLL0MT: res = "WS_DLL0MT";
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

InputRateController* NewInputRateController(){
  std::unique_ptr<InputRateController> input_rate_controller(new InputRateController());
  return input_rate_controller.release();
}

}