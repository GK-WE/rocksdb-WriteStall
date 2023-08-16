//
// Created by Haoyu Gong on 2023.
//

#include <atomic>
#include "rocksdb/env.h"
#include "db/column_family.h"
#include "rocksdb/status.h"

namespace ROCKSDB_NAMESPACE{
class InputRateController{
 public:
  explicit InputRateController();

  ~InputRateController();

  enum BackgroundOp_Priority {
    IO_STOP = 0,
    IO_LOW = 1,
    IO_HIGH = 2,
    IO_TOTAL
  };

  // bitwise 000 --> (DL,L0,MT)
  enum WriteStall_Condition{
    WS_NORMAL = 0,   // 000
    WS_MT = 1,       // 001
    WS_L0 = 2,       // 010
    WS_L0MT = 3,     // 011
    WS_DL = 4,       // 100
    WS_DLMT = 5,     // 101
    WS_DLL0 = 6,     // 110
    WS_DLL0MT = 7,    // 111
    WS_TOTAL = 8
  };

  int DecideWriteStallCondition(ColumnFamilyData* cfd, const MutableCFOptions& mutable_cf_options);

  BackgroundOp_Priority DecideBackgroundOpPriority(ColumnFamilyData* cfd, Env::BackgroundOp background_op, const MutableCFOptions& mutable_cf_options);

  Env::BackgroundOp DecideStoppedBackgroundOp(ColumnFamilyData* cfd, const MutableCFOptions& mutable_cf_options);

  size_t RequestToken(size_t bytes, size_t alignment,
                      ColumnFamilyData* cfd, Env::BackgroundOp background_op, const MutableCFOptions& mutable_cf_options);

  void Request(size_t bytes, ColumnFamilyData* cfd, Env::BackgroundOp background_op, const MutableCFOptions& mutable_cf_options);

  void ReturnToken(ColumnFamilyData* cfd, Env::BackgroundOp background_op,
                   const MutableCFOptions& mutable_cf_options);

  std::string BackgroundOpPriorityString(BackgroundOp_Priority io_pri);

  std::string BackgroundOpString(Env::BackgroundOp op);

 private:
  std::shared_ptr<SystemClock> clock_;
  std::atomic<int> cur_high_;
  std::atomic<int> timeout_low_;
  mutable port::Mutex request_mutex_;
  port::CondVar exit_cv_;
  int32_t requests_to_wait_;
  struct Req;
  std::deque<Req*> stopped_bkop_queue_[Env::BK_TOTAL];
  std::deque<Req*> low_bkop_queue_;
  bool stop_;

};
extern InputRateController* NewInputRateController();
}