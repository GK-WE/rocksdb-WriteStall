//
// Created by 龚皓宇 on 8/15/23.
//
#include "rocksdb/env.h"

namespace ROCKSDB_NAMESPACE{
class InputRateController{
 public:
  explicit InputRateController();

  ~InputRateController();

// private:
//  std::shared_ptr<SystemClock> clock_;
};
extern InputRateController* NewInputRateController();
}