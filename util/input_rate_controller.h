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

  struct Req;

  enum BackgroundOp_Priority {
    IO_STOP = 0,
    IO_LOW = 1,
    IO_HIGH = 2,
    IO_TOTAL
  };

  // bitwise 000 --> (DL,L0,MT)
  enum ComponentConstraint_Condition{
    CCV_NORMAL = 0,   // 000
    CCV_MT = 1,       // 001
    CCV_L0 = 2,       // 010
    CCV_L0MT = 3,     // 011
    CCV_DL = 4,       // 100
    CCV_DLMT = 5,     // 101
    CCV_DLL0 = 6,     // 110
    CCV_DLL0MT = 7,    // 111
  };

  enum ComponentConstraintViolation_Cushion{
    CUSHION_NORMAL = 0,
    CUSHION_L0 = 1, // prev state include CCV_L0, but now L0 is not decreased to a safty value
    CUSHION_DL = 2, //pre state include CCV_DL, but now DL is not decreased to a safy value
    CUSHION_DLL0 = 3, // both L0 and DL
    CUSHION_TOTAL
  };

  enum ThreadSignaledReason{
    TSREASON_CCV_CHANGE = 0,
    TSREASON_TIMEOUT = 1,
    TSREASON_NOCMP_DLCC = 2,
    TSREASON_ZERO_HIGH = 3,
    TSREASON_SHUTDOWN =4,
    TSREASON_TOTAL = 5
  };

  void DecideIfNeedRequestReturnToken(ColumnFamilyData* cfd, Env::BackgroundOp background_op, const MutableCFOptions& mutable_cf_options, bool& need_request_token, bool& need_return_token);

  size_t RequestToken(size_t bytes, size_t alignment,
                      ColumnFamilyData* cfd, Env::BackgroundOp background_op, const MutableCFOptions& mutable_cf_options);

  void ReturnToken(ColumnFamilyData* cfd, Env::BackgroundOp background_op);

  void SignalStopOpWhenNoCmpButDLCC(ColumnFamilyData* cfd);

  void SetCmpNoWhenDLCC(bool nocmp);

  bool GetCmpNoWhenDLCC(){return compaction_nothing_todo_when_dlcc_.load(std::memory_order_relaxed);};

  static int DecideCurWriteStallCondition(ColumnFamilyData* cfd, const MutableCFOptions& mutable_cf_options);

  static int DecideCurDiskWriteStallCondition(VersionStorageInfo* vstorage,const MutableCFOptions& mutable_cf_options);

 private:
  int DecideWriteStallChange(ColumnFamilyData* cfd, const MutableCFOptions& mutable_cf_options, int cur_ws);

  BackgroundOp_Priority DecideBackgroundOpPriority( Env::BackgroundOp background_op, int cur_ws,int cushion);

  Env::BackgroundOp DecideStoppedBackgroundOp(int cur_ws,int cushion);

  void Request(size_t bytes, ColumnFamilyData* cfd, Env::BackgroundOp background_op, const MutableCFOptions& mutable_cf_options);

  void UpdatePrevCCVCondition(int exp, int cur){
      prev_write_stall_condition_.compare_exchange_strong(exp,cur);
  }

  static std::string BackgroundOpPriorityString(BackgroundOp_Priority io_pri);

  static std::string BackgroundOpString(Env::BackgroundOp op);

  static std::string CCVConditionString(int ws);

  static std::string CushionString(int cu);

  static std::string TSReasonString(int ts);

  void SignalStopOpExcept(ColumnFamilyData* cfd, Env::BackgroundOp except_op, Env::BackgroundOp cur_op, BackgroundOp_Priority io_pri);

  void SignalLowOpShouldBeHighOpNow(ColumnFamilyData* cfd, Env::BackgroundOp background_op);

  std::shared_ptr<SystemClock> clock_;
  std::atomic<int> cur_high_;
  std::atomic<int> prev_write_stall_condition_;
  mutable port::Mutex request_mutex_;
  port::CondVar exit_cv_;
  int32_t requests_to_wait_;
  std::deque<Req*> stopped_bkop_queue_[Env::BK_TOTAL];
  std::deque<Req*> low_bkop_queue_;
  bool stop_;
  std::atomic<bool> compaction_nothing_todo_when_dlcc_;
  bool need_debug_info_;

};
extern InputRateController* NewInputRateController();
}