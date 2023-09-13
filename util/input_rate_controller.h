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

  struct InfoCFD;

  enum BackgroundOp_Priority {
    IO_STOP = 0,
    IO_LOW = 1,
    IO_HIGH = 2,
    IO_TOTAL
  };

  // bitwise 000 --> (EC,L0,MT)
  enum ComponentConstraint_Condition{
    CC_NORMAL = 0,   // 000
    CC_MT = 1,       // 001
    CC_L0 = 2,       // 010
    CC_L0MT = 3,     // 011
    CC_EC = 4,       // 100
    CC_ECMT = 5,     // 101
    CC_ECL0 = 6,     // 110
    CC_ECL0MT = 7,    // 111
    CC_TOTAL = 8
  };

  enum ComponentConstraintViolation_Cushion{
    CUSHION_NORMAL = 0,
    CUSHION_L0 = 1, // prev state include CC_L0, but now L0 is not decreased to a safe value
    CUSHION_EC = 2, //pre state include CC_EC, but now EC is not decreased to a safe value
    CUSHION_ECL0 = 3, // both L0 and EC
    CUSHION_TOTAL
  };

  enum ThreadSignaledReason{
    TSREASON_CC_CHANGE = 0,
    TSREASON_TIMEOUT = 1,
    TSREASON_ZERO_HIGH = 2,
    TSREASON_SHUTDOWN =3,
    TSREASON_TOTAL
  };

  void DecideIfNeedRequestAndReturnToken(ColumnFamilyData* cfd, Env::BackgroundOp background_op, bool& need_request_token, bool& need_return_token);

  size_t RequestToken(size_t bytes, size_t alignment,
                      ColumnFamilyData* cfd, Env::BackgroundOp background_op);

  void ReturnToken(ColumnFamilyData* cfd, Env::BackgroundOp background_op);

//  static int DecideCurWriteStallCondition(ColumnFamilyData* cfd, const MutableCFOptions& mutable_cf_options);

  void SetCurCFDInfo(std::string name, VersionStorageInfo *vstorage, int mem, const MutableCFOptions& mutable_cf_options);

 private:
  void UpdateCC();

  void UpdateCushion();

  void UpdateStoppedOp();

//  int DecideWriteStallChange(ColumnFamilyData* cfd, const MutableCFOptions& mutable_cf_options, int cur_ws);

  BackgroundOp_Priority DecideBackgroundOpPriority( Env::BackgroundOp background_op, std::string name);

//  Env::BackgroundOp DecideStoppedBackgroundOp(int cur_ws,int cushion);

  void Request(size_t bytes, ColumnFamilyData* cfd, Env::BackgroundOp background_op);

    static std::string BackgroundOpPriorityString(BackgroundOp_Priority io_pri);

  static std::string BackgroundOpString(Env::BackgroundOp op);

  static std::string CCConditionString(int ws);

  static std::string CushionString(int cu);

  static std::string TSReasonString(int ts);

//  void SignalStopOpExcept(ColumnFamilyData* cfd, Env::BackgroundOp except_op, Env::BackgroundOp cur_op, BackgroundOp_Priority io_pri);
//
//  void SignalLowOpShouldBeHighOpNow(ColumnFamilyData* cfd, Env::BackgroundOp background_op);

  std::shared_ptr<SystemClock> clock_;
  std::atomic<int> cur_high_;
  mutable port::Mutex request_mutex_;
  port::CondVar exit_cv_;
  int32_t requests_to_wait_;
  std::map<std::string, std::deque<Req*>[Env::BK_TOTAL]> stopped_bkop_queue_;
//  std::deque<Req*> stopped_bkop_queue_[Env::BK_TOTAL];
  std::map<std::string, std::deque<Req*>> low_bkop_queue_;
//  std::deque<Req*> low_bkop_queue_;
  bool stop_;
  std::map<std::string,InfoCFD*> cur_cfd_info;
  std::map<std::string,InfoCFD*> prev_cfd_info;
//  std::map<std::string,ComponentConstraint_Condition> cur_cc;
//  std::map<std::string,ComponentConstraint_Condition> prev_cc;
//  std::map<std::string,ComponentConstraintViolation_Cushion> cushion;
  std::map<std::string,int> cur_cc;
  std::map<std::string,int> prev_cc;
  std::map<std::string,int> cushion;
  std::map<std::string,Env::BackgroundOp> stopped_op;
  bool need_debug_info_;

};
extern InputRateController* NewInputRateController();
}