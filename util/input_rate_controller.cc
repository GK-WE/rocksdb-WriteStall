//
// Created by 龚皓宇 on 8/15/23.
//

#include "util/input_rate_controller.h"

namespace ROCKSDB_NAMESPACE{
InputRateController::InputRateController(){}

InputRateController::~InputRateController(){}

InputRateController* NewInputRateController() {
  std::unique_ptr<InputRateController> input_rate_controller(new InputRateController());
  return input_rate_controller.release();
}
}