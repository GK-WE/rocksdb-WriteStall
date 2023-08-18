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
  };

  enum WriteStall_Cushion{
    CUSHION_NORMAL = 0,
    CUSHION_L0 = 1, // prev state include WS_L0, but now L0 is not decreased to a safty value
    CUSHION_DL = 2, //pre state include WS_DL, but now DL is not decreased to a safy value
    CUSHION_DLL0 = 3, // both L0 and DL
    CUSHION_TOTAL
  };

  void DecideIfNeedRequestReturnToken(ColumnFamilyData* cfd, Env::BackgroundOp background_op, const MutableCFOptions& mutable_cf_options, bool& need_request_token, bool& need_return_token);

  size_t RequestToken(size_t bytes, size_t alignment,
                      ColumnFamilyData* cfd, Env::BackgroundOp background_op, const MutableCFOptions& mutable_cf_options);

  void ReturnToken(ColumnFamilyData* cfd, Env::BackgroundOp background_op);

  static std::string BackgroundOpPriorityString(BackgroundOp_Priority io_pri);

  static std::string BackgroundOpString(Env::BackgroundOp op);

  static std::string WSConditionString(int ws);

  static std::string CushionString(int cu);

 private:
  static int DecideCurWriteStallCondition(ColumnFamilyData* cfd, const MutableCFOptions& mutable_cf_options);

  int DecideWriteStallChange(ColumnFamilyData* cfd, const MutableCFOptions& mutable_cf_options, int cur_ws);

  static BackgroundOp_Priority DecideBackgroundOpPriority( Env::BackgroundOp background_op, int cur_ws,int cushion);

  static Env::BackgroundOp DecideStoppedBackgroundOp(int cur_ws,int cushion);

  void Request(size_t bytes, ColumnFamilyData* cfd, Env::BackgroundOp background_op, const MutableCFOptions& mutable_cf_options);

  void UpdatePrevWSCondition(int cur){
    prev_write_stall_condition_.store(cur);
  }

  std::shared_ptr<SystemClock> clock_;
  std::atomic<int> cur_high_;
  std::atomic<int> prev_write_stall_condition_;
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