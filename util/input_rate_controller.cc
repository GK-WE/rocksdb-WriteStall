//
// Created by Haoyu Gong on 2023.
//

#include "input_rate_controller.h"
#include "db/column_family.h"
#include "db/version_set.h"
#include "logging/logging.h"

namespace ROCKSDB_NAMESPACE{
const int64_t low_bkop_max_wait_us = 100000; // 1/10 sec
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
timeout_low_(0),
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

int InputRateController::DecideWriteStallCondition(ColumnFamilyData* cfd,
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
    bool PS = (estimated_compaction_needed_bytes >= mutable_cf_options.hard_pending_compaction_bytes_limit);
    result = (MT ? 1 : 0) + (L0 ? 2 : 0) + (PS ? 4 : 0);
  }
  return result;
}

InputRateController::BackgroundOp_Priority InputRateController::DecideBackgroundOpPriority(ColumnFamilyData* cfd,
                                                                                              Env::BackgroundOp background_op,
                                                                                              const MutableCFOptions& mutable_cf_options) {
  InputRateController::BackgroundOp_Priority io_pri;
  int result = DecideWriteStallCondition(cfd,mutable_cf_options);
  switch (result) {
    case WS_NORMAL: io_pri = (background_op == Env::BK_DLCMP)? IO_LOW: IO_HIGH;
    break;
    case WS_MT: io_pri = (background_op == Env::BK_FLUSH)? IO_HIGH: IO_LOW; // no background op stopped; Flush HIGH; other LOW
    break;
    case WS_L0: io_pri = (background_op == Env::BK_L0CMP)? IO_HIGH: IO_LOW; // Flush stopped;
    break;
    case WS_L0MT: io_pri = (background_op == Env::BK_DLCMP)? IO_LOW: IO_HIGH; // no background op stopped;
    break;
    case WS_DL: io_pri = (background_op == Env::BK_DLCMP)? IO_HIGH: IO_LOW; // L0-L1 compaction stopped;
    break;
    case WS_DLMT: io_pri = (background_op == Env::BK_L0CMP)? IO_LOW: IO_HIGH; // L0-L1 compaction stopped;
    break;
    case WS_DLL0: io_pri = (background_op == Env::BK_FLUSH)? IO_LOW: IO_HIGH; // Flush stopped;
    break;
    case WS_DLL0MT: io_pri = (background_op == Env::BK_DLCMP)? IO_HIGH: IO_LOW;
    break;
    default: io_pri = IO_TOTAL;
    break;
  }
  return io_pri;
}

Env::BackgroundOp InputRateController::DecideStoppedBackgroundOp(ColumnFamilyData* cfd,
                                                                    const MutableCFOptions& mutable_cf_options) {
  Env::BackgroundOp stopped_op;
  int result = DecideWriteStallCondition(cfd,mutable_cf_options);
  switch (result) {
    case WS_NORMAL: stopped_op = Env::BK_TOTAL;
    break;
    case WS_MT: stopped_op = Env::BK_TOTAL;
    break;
    case WS_L0: stopped_op = Env::BK_FLUSH;
    break;
    case WS_L0MT: stopped_op = Env::BK_TOTAL;
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

void InputRateController::ReturnToken(ColumnFamilyData* cfd, Env::BackgroundOp background_op,
                                      const MutableCFOptions& mutable_cf_options) {
  InputRateController::BackgroundOp_Priority io_pri = DecideBackgroundOpPriority(cfd,background_op,mutable_cf_options);
  ROCKS_LOG_INFO(cfd->ioptions()->logger,"[%s] InputRateController::ReturnToken: backgroundop: %s io_pri: %s ", cfd->GetName().c_str(),
                 BackgroundOpString(background_op).c_str(), BackgroundOpPriorityString(io_pri).c_str());
  if(io_pri!=IO_HIGH) {
    return;
  }
  MutexLock g(&request_mutex_);
  if(io_pri == IO_HIGH){
    --cur_high_;
  }
  if(cur_high_==0 && !low_bkop_queue_.empty()){
    Req* r = low_bkop_queue_.front();
    r->cv.Signal();
  }
}

void InputRateController::Request(size_t bytes, ColumnFamilyData* cfd,
                                  Env::BackgroundOp background_op,
                                  const MutableCFOptions& mutable_cf_options) {
  InputRateController::BackgroundOp_Priority io_pri = DecideBackgroundOpPriority(cfd,background_op,mutable_cf_options);
  ROCKS_LOG_INFO(cfd->ioptions()->logger,"[%s] InputRateController::RequestToken: backgroundop: %s io_pri: %s bytes: %zu ", cfd->GetName().c_str(),
                 BackgroundOpString(background_op).c_str(), BackgroundOpPriorityString(io_pri).c_str(), bytes);
  if(io_pri==IO_TOTAL) {
    return;
  }
  Env::BackgroundOp stopped_op = DecideStoppedBackgroundOp(cfd, mutable_cf_options);


  MutexLock g(&request_mutex_);

  for (int i = Env::BK_TOTAL - 1; i >= Env::BK_FLUSH; --i) {
    if(i==stopped_op){
      continue;
    }
    std::deque<Req*> queue = stopped_bkop_queue_[i];
    while (!queue.empty()) {
      queue.front()->cv.Signal();
      queue.pop_front();
    }
  }

  if(background_op == stopped_op){
    Req r(bytes, &request_mutex_, background_op);
    stopped_bkop_queue_[stopped_op].push_back(&r);
    r.cv.Wait();
  }else if(io_pri == IO_HIGH){
    ++cur_high_;
    std::deque<Req*> queue;
    for(auto& r: low_bkop_queue_){
      if(r->background_op == background_op){
        queue.push_back(r);
      }
    }
    for(auto&r : queue){
      r->cv.Signal();
    }
  }else if(io_pri == IO_LOW){
    Req r(bytes, &request_mutex_, background_op);
    low_bkop_queue_.push_back(&r);
    bool timeout_occurred = false;
    if(background_op==Env::BK_DLCMP){
      while(cur_high_>0){
        r.cv.Wait();
      }
    }else{
      while(cur_high_>0 && !timeout_occurred){
        int64_t wait_until = clock_->NowMicros() + low_bkop_max_wait_us;
        timeout_occurred = r.cv.TimedWait(wait_until);
      }
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

InputRateController* NewInputRateController(){
  std::unique_ptr<InputRateController> input_rate_controller(new InputRateController());
  return input_rate_controller.release();
}

}