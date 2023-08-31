//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under both the GPLv2 (found in the
//  COPYING file in the root directory) and Apache 2.0 License
//  (found in the LICENSE.Apache file in the root directory).
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <string>
#include <utility>
#include <vector>

#include "db/compaction/compaction_picker_level.h"
#include "logging/log_buffer.h"
#include "test_util/sync_point.h"
#include "logging/logging.h"
#include "util/input_rate_controller.h"

namespace ROCKSDB_NAMESPACE {

bool LevelCompactionPicker::NeedsCompaction(
    const VersionStorageInfo* vstorage) const {
  if (!vstorage->ExpiredTtlFiles().empty()) {
    return true;
  }
  if (!vstorage->FilesMarkedForPeriodicCompaction().empty()) {
    return true;
  }
  if (!vstorage->BottommostFilesMarkedForCompaction().empty()) {
    return true;
  }
  if (!vstorage->FilesMarkedForCompaction().empty()) {
    return true;
  }
  if (!vstorage->FilesMarkedForForcedBlobGC().empty()) {
    return true;
  }
  for (int i = 0; i <= vstorage->MaxInputLevel(); i++) {
    if (vstorage->CompactionScore(i) >= 1) {
      return true;
    }
  }
  return false;
}

namespace {
// A class to build a leveled compaction step-by-step.
class LevelCompactionBuilder {
 public:
  LevelCompactionBuilder(const std::string& cf_name,
                         VersionStorageInfo* vstorage,
                         SequenceNumber earliest_mem_seqno,
                         CompactionPicker* compaction_picker,
                         LogBuffer* log_buffer,
                         const MutableCFOptions& mutable_cf_options,
                         const ImmutableOptions& ioptions,
                         const MutableDBOptions& mutable_db_options)
      : cf_name_(cf_name),
        vstorage_(vstorage),
        earliest_mem_seqno_(earliest_mem_seqno),
        compaction_picker_(compaction_picker),
        log_buffer_(log_buffer),
        mutable_cf_options_(mutable_cf_options),
        ioptions_(ioptions),
        mutable_db_options_(mutable_db_options) {}

  // Pick and return a compaction.
  Compaction* PickCompaction();

  //Pick and return a multi level compaction when DL-CC is reached
  Compaction* PickMLOCompaction();

  //Pick and return files at a level that can trivial move to next level
//  Compaction* PickBatchTrivialMoveCompaction();

  bool SetupLevelsMLOCompaction();

  void SetupInitialFilesMLOCompaction();

//  void SetupInitialFilesTrivialMoveCompaction();

//  bool PickStartLevelFilesTrivialMoveCompaction();

  std::string LogInputFilesInfo(CompactionInputFiles inputs);

  bool SetupOtherInputsMLOCompaction();

  void LogCompactionScoreInfo();

  std::string LogFilesByCompactionPri(int level, std::vector<int> files) ;

  // Pick the initial files to compact to the next level. (or together
  // in Intra-L0 compactions)
  void SetupInitialFiles();

  // If the initial files are from L0 level, pick other L0
  // files if needed.
  bool SetupOtherL0FilesIfNeeded();

  // Based on initial files, setup other files need to be compacted
  // in this compaction, accordingly.
  bool SetupOtherInputsIfNeeded();

  Compaction* GetCompaction();

  // For the specfied level, pick a file that we want to compact.
  // Returns false if there is no file to compact.
  // If it returns true, inputs->files.size() will be exactly one.
  // If level is 0 and there is already a compaction on that level, this
  // function will return false.
  bool PickFileToCompact();

  // For L0->L0, picks the longest span of files that aren't currently
  // undergoing compaction for which work-per-deleted-file decreases. The span
  // always starts from the newest L0 file.
  //
  // Intra-L0 compaction is independent of all other files, so it can be
  // performed even when L0->base_level compactions are blocked.
  //
  // Returns true if `inputs` is populated with a span of files to be compacted;
  // otherwise, returns false.
  bool PickIntraL0Compaction();

  // Picks a file from level_files to compact.
  // level_files is a vector of (level, file metadata) in ascending order of
  // level. If compact_to_next_level is true, compact the file to the next
  // level, otherwise, compact to the same level as the input file.
  void PickFileToCompact(
      const autovector<std::pair<int, FileMetaData*>>& level_files,
      bool compact_to_next_level);

  const std::string& cf_name_;
  VersionStorageInfo* vstorage_;
  SequenceNumber earliest_mem_seqno_;
  CompactionPicker* compaction_picker_;
  LogBuffer* log_buffer_;
  int start_level_ = -1;
  int output_level_ = -1;
  std::vector<int> mlo_compaction_levels;
  int parent_index_ = -1;
  int base_index_ = -1;
  double start_level_score_ = 0;
  bool is_manual_ = false;
  CompactionInputFiles start_level_inputs_;
  std::vector<CompactionInputFiles> compaction_inputs_;
  CompactionInputFiles output_level_inputs_;
  CompactionInputFiles mid_level_inputs1_;
//  CompactionInputFiles mid_level_inputs2_;
  std::vector<FileMetaData*> grandparents_;
  CompactionReason compaction_reason_ = CompactionReason::kUnknown;

  const MutableCFOptions& mutable_cf_options_;
  const ImmutableOptions& ioptions_;
  const MutableDBOptions& mutable_db_options_;
  // Pick a path ID to place a newly generated file, with its level
  static uint32_t GetPathId(const ImmutableCFOptions& ioptions,
                            const MutableCFOptions& mutable_cf_options,
                            int level);

  static const int kMinFilesForIntraL0Compaction = 4;
};

void LevelCompactionBuilder::LogCompactionScoreInfo() {
  std::string compaction_score = "[";
  std::string compaction_level = "[";
  std::string level_files_num = "[";
  for (int i = 0; i < compaction_picker_->NumberLevels() - 1; i++){
    compaction_level += " ";
    compaction_score += " ";
    level_files_num += " ";
    compaction_level += std::to_string(vstorage_->CompactionScoreLevel(i));
    compaction_score += std::to_string(vstorage_->CompactionScore(i));
    level_files_num += std::to_string(vstorage_->NumLevelFiles(
        vstorage_->CompactionScoreLevel(i)));
  }
  compaction_level += " ]";
  compaction_score += " ]";
  level_files_num += " ]";
  ROCKS_LOG_BUFFER(
      log_buffer_,
      "[%s] CompactionPriorityInformation: CompactionLevel: %s "
      "CompactionScore: %s "
      "LevelFileNum: %s ", cf_name_.c_str(),
      compaction_level.c_str(),
      compaction_score.c_str(),
      level_files_num.c_str());
}

void LevelCompactionBuilder::PickFileToCompact(
    const autovector<std::pair<int, FileMetaData*>>& level_files,
    bool compact_to_next_level) {
  for (auto& level_file : level_files) {
    // If it's being compacted it has nothing to do here.
    // If this assert() fails that means that some function marked some
    // files as being_compacted, but didn't call ComputeCompactionScore()
    assert(!level_file.second->being_compacted);
    start_level_ = level_file.first;
    if ((compact_to_next_level &&
         start_level_ == vstorage_->num_non_empty_levels() - 1) ||
        (start_level_ == 0 &&
         !compaction_picker_->level0_compactions_in_progress()->empty())) {
      continue;
    }
    if (compact_to_next_level) {
      output_level_ =
          (start_level_ == 0) ? vstorage_->base_level() : start_level_ + 1;
    } else {
      output_level_ = start_level_;
    }
    start_level_inputs_.files = {level_file.second};
    start_level_inputs_.level = start_level_;
    if (compaction_picker_->ExpandInputsToCleanCut(cf_name_, vstorage_,
                                                   &start_level_inputs_)) {
      return;
    }
  }
  start_level_inputs_.files.clear();
}

void LevelCompactionBuilder::SetupInitialFiles() {
  // Find the compactions by size on all levels.
  bool skipped_l0_to_base = false;
//  LogCompactionScoreInfo();
  int ccv = InputRateController::DecideCurDiskWriteStallCondition(vstorage_,
                                                                  mutable_cf_options_);
  bool dl_ccv = (ccv >> 2) & 1;
  for (int i = 0; i < compaction_picker_->NumberLevels() - 1; i++) {
    start_level_score_ = vstorage_->CompactionScore(i);
    start_level_ = vstorage_->CompactionScoreLevel(i);
    assert(i == 0 || start_level_score_ <= vstorage_->CompactionScore(i - 1));
    if (start_level_score_ >= 1) {

      if(ioptions_.input_rate_cotroller_enabled){
        if(start_level_ == 0 && dl_ccv){
          if((i + 1) < (compaction_picker_->NumberLevels() - 1)
              && vstorage_->CompactionScore(i + 1) > 1){
            continue;
          }
        }
      }

      if (skipped_l0_to_base && start_level_ == vstorage_->base_level()) {
        // If L0->base_level compaction is pending, don't schedule further
        // compaction from base level. Otherwise L0->base_level compaction
        // may starve.
        if((ioptions_.input_rate_cotroller_enabled && !dl_ccv
             && !start_level_inputs_.abandon_outputlevel_toolarge) ||
            (!ioptions_.input_rate_cotroller_enabled)){
          ROCKS_LOG_BUFFER(
              log_buffer_,
              "[%s] Starving L1-L2 Compaction due to pending L0-L1 Compaction: "
              "DL-CCV: %s TooLargeL1: %s input_rate_cotroller_enabled: %s ",
              cf_name_.c_str(),
              dl_ccv?"true":"false",
              start_level_inputs_.abandon_outputlevel_toolarge?"true":"false",
              ioptions_.input_rate_cotroller_enabled?"true":"false");
          continue;
        }
      }
      output_level_ =
          (start_level_ == 0) ? vstorage_->base_level() : start_level_ + 1;
      if (PickFileToCompact()) {
        // found the compaction!
        if (start_level_ == 0) {
          // L0 score = `num L0 files` / `level0_file_num_compaction_trigger`
          compaction_reason_ = CompactionReason::kLevelL0FilesNum;
        } else {
          // L1+ score = `Level files size` / `MaxBytesForLevel`
          compaction_reason_ = CompactionReason::kLevelMaxLevelSize;
        }
        break;
      } else {
        // didn't find the compaction, clear the inputs
        start_level_inputs_.clear();
        if (start_level_ == 0) {
          skipped_l0_to_base = true;
          // L0->base_level may be blocked due to ongoing L0->base_level
          // compactions. It may also be blocked by an ongoing compaction from
          // base_level downwards.
          //
          // In these cases, to reduce L0 file count and thus reduce likelihood
          // of write stalls, we can attempt compacting a span of files within
          // L0.
          if (PickIntraL0Compaction()) {
            output_level_ = 0;
            compaction_reason_ = CompactionReason::kLevelL0FilesNum;
            break;
          }
        }
      }
    } else {
      // Compaction scores are sorted in descending order, no further scores
      // will be >= 1.
      break;
    }
  }
  if (!start_level_inputs_.empty()) {
    return;
  }

  // if we didn't find a compaction, check if there are any files marked for
  // compaction
  parent_index_ = base_index_ = -1;

  compaction_picker_->PickFilesMarkedForCompaction(
      cf_name_, vstorage_, &start_level_, &output_level_, &start_level_inputs_);
  if (!start_level_inputs_.empty()) {
    compaction_reason_ = CompactionReason::kFilesMarkedForCompaction;
    return;
  }

  // Bottommost Files Compaction on deleting tombstones
  PickFileToCompact(vstorage_->BottommostFilesMarkedForCompaction(), false);
  if (!start_level_inputs_.empty()) {
    compaction_reason_ = CompactionReason::kBottommostFiles;
    return;
  }

  // TTL Compaction
  PickFileToCompact(vstorage_->ExpiredTtlFiles(), true);
  if (!start_level_inputs_.empty()) {
    compaction_reason_ = CompactionReason::kTtl;
    return;
  }

  // Periodic Compaction
  PickFileToCompact(vstorage_->FilesMarkedForPeriodicCompaction(), false);
  if (!start_level_inputs_.empty()) {
    compaction_reason_ = CompactionReason::kPeriodicCompaction;
    return;
  }

  // Forced blob garbage collection
  PickFileToCompact(vstorage_->FilesMarkedForForcedBlobGC(), false);
  if (!start_level_inputs_.empty()) {
    compaction_reason_ = CompactionReason::kForcedBlobGC;
    return;
  }
}

bool LevelCompactionBuilder::SetupOtherL0FilesIfNeeded() {
  if (start_level_ == 0 && output_level_ != 0) {
    return compaction_picker_->GetOverlappingL0Files(
        vstorage_, &start_level_inputs_, output_level_, &parent_index_);
  }
  return true;
}

bool LevelCompactionBuilder::SetupOtherInputsIfNeeded() {
  // Setup input files from output level. For output to L0, we only compact
  // spans of files that do not interact with any pending compactions, so don't
  // need to consider other levels.
  if (output_level_ != 0) {
    output_level_inputs_.level = output_level_;
    if (!compaction_picker_->SetupOtherInputs(
            cf_name_, mutable_cf_options_, vstorage_, &start_level_inputs_,
            &output_level_inputs_, &parent_index_, base_index_)) {
      return false;
    }

    compaction_inputs_.push_back(start_level_inputs_);
    if (!output_level_inputs_.empty()) {
      compaction_inputs_.push_back(output_level_inputs_);
    }

    // In some edge cases we could pick a compaction that will be compacting
    // a key range that overlap with another running compaction, and both
    // of them have the same output level. This could happen if
    // (1) we are running a non-exclusive manual compaction
    // (2) AddFile ingest a new file into the LSM tree
    // We need to disallow this from happening.
    if (compaction_picker_->FilesRangeOverlapWithCompaction(compaction_inputs_,
                                                            output_level_)) {
      // This compaction output could potentially conflict with the output
      // of a currently running compaction, we cannot run it.
      return false;
    }
    compaction_picker_->GetGrandparents(vstorage_, start_level_inputs_,
                                        output_level_inputs_, &grandparents_);
  } else {
    compaction_inputs_.push_back(start_level_inputs_);
  }
  return true;
}

std::string LevelCompactionBuilder::LogInputFilesInfo(CompactionInputFiles inputs) {
  std::string res = "Files_L" + std::to_string(inputs.level) + ": [";
  for(auto f: inputs.files){
    res += " ";
    res += std::to_string(f->fd.GetNumber());
  }
  res += " ]";
  return res;
}

std::string LevelCompactionBuilder::LogFilesByCompactionPri(int level, std::vector<int> idx) {
  std::string res = "Files_L" + std::to_string(level) + ": [";
  std::vector<FileMetaData*> files = vstorage_->LevelFiles(level);
  for(auto i: idx){
    res += " ";
    uint64_t  fnum = files[i]->fd.GetNumber();
    if(files[i]->being_compacted){
      res += "(" + std::to_string(fnum) + ")";
    }else{
      res += std::to_string(fnum);
    }
  }
  res += " ]";
  return res;
}

bool LevelCompactionBuilder::SetupLevelsMLOCompaction() {
  mlo_compaction_levels.clear();
  //find at least 2 consecutive levels needed compaction
  // the last one that don't need compaction as output level
  for(int i = 1; i < compaction_picker_->NumberLevels() - 1; i++){
    double level_score = vstorage_->CompactionLevelScore(i);
    if(level_score>1){
      mlo_compaction_levels.push_back(i);
    }else{
      if(mlo_compaction_levels.size()>=2){
        mlo_compaction_levels.push_back(i);
        break;
      }else{
        mlo_compaction_levels.clear();
        continue;
      }
    }
    if(mlo_compaction_levels.size()==3){
      break;
    }
  }

  if(mlo_compaction_levels.size()>2){
    start_level_ = mlo_compaction_levels.front();
    output_level_ = mlo_compaction_levels.back();
  }else{
    ROCKS_LOG_BUFFER(log_buffer_, "PickMLOCompaction: action: SetupLevels "
                     "result: FAILED "
                     "reason: LEVELS-NOT-ENOUGH ");
    return false;
  }

  std::string mlo_levels = "[";
  for(const int l: mlo_compaction_levels){
    mlo_levels += " ";
    mlo_levels += std::to_string(l);
  }
  mlo_levels += " ]";
  ROCKS_LOG_BUFFER(log_buffer_, "PickMLOCompaction: action: SetupLevels "
                                "result: SUCCESS "
                                "picked_levels: %s ", mlo_levels.c_str());
  return true;
}

void LevelCompactionBuilder::SetupInitialFilesMLOCompaction() {
  assert(start_level_>0);
  //select files from start_level_
  start_level_inputs_.clear();
  const std::vector<int>& file_size =
      vstorage_->FilesByCompactionPri(start_level_);
  const std::vector<FileMetaData*>& level_files =
      vstorage_->LevelFiles(start_level_);
  unsigned int cmp_idx;
  for (cmp_idx = vstorage_->NextCompactionIndex(start_level_);
      cmp_idx < file_size.size(); cmp_idx++) {
    int index = file_size[cmp_idx];
    auto* f = level_files[index];
    if (f->being_compacted) {
      start_level_inputs_.clear();
      continue;
    }
    start_level_inputs_.files.push_back(f);
    start_level_inputs_.level = start_level_;
    if (!compaction_picker_->ExpandInputsToCleanCut(cf_name_, vstorage_,
                                                    &start_level_inputs_) ||
                                                    compaction_picker_->FilesRangeOverlapWithCompaction(
                                                        {start_level_inputs_}, mlo_compaction_levels[1])) {
      // A locked (pending compaction) input-level file was pulled in due to
      // user-key overlap.
      start_level_inputs_.clear();
      continue;
    }

    InternalKey smallest, largest;
    compaction_picker_->GetRange(start_level_inputs_, &smallest, &largest);
    CompactionInputFiles output_level_inputs;
    output_level_inputs.level = start_level_+1;
    vstorage_->GetOverlappingInputs(start_level_+1, &smallest, &largest,
                                    &output_level_inputs.files);
    if (!output_level_inputs.empty() &&
    !compaction_picker_->ExpandInputsToCleanCut(cf_name_, vstorage_,
                                                &output_level_inputs)) {
      start_level_inputs_.clear();
      continue;
    }

    base_index_ = index;
    break;
  }
  vstorage_->SetNextCompactionIndex(start_level_, cmp_idx);
}

bool LevelCompactionBuilder::SetupOtherInputsMLOCompaction() {
  ROCKS_LOG_BUFFER(log_buffer_, "PickMLOCompaction: action: SetupOtherInputs-START ");
  ROCKS_LOG_BUFFER(log_buffer_, "PickMLOCompaction: action: BeforeSetupMidLevelInputs1 "
                                "start_level: [level: %d , num: %d] %s ",
                                start_level_inputs_.level,
                                start_level_inputs_.size(),
                   LogInputFilesInfo(start_level_inputs_).c_str());
  size_t old_startlevel_inputs_num = start_level_inputs_.size();
  mid_level_inputs1_.level = mlo_compaction_levels[1];
  if(!compaction_picker_->SetupOtherInputs(
      cf_name_, mutable_cf_options_, vstorage_, &start_level_inputs_,
      &mid_level_inputs1_, &parent_index_, base_index_) || mid_level_inputs1_.empty()){
    ROCKS_LOG_BUFFER(log_buffer_, "PickMLOCompaction: action: AfterSetupMidLevelInputs1 "
                                  "start_level: [level: %d , num: %d] %s "
                                  "mid_level1: [level: %d , num: %d] %s "
                     "result: FAILED "
                     "reason: SETUP-OTHER-INPUTS | MID-INPUT-EMPTY",
                     start_level_inputs_.level,
                     start_level_inputs_.size(),
                     LogInputFilesInfo(start_level_inputs_).c_str(),
                     mid_level_inputs1_.level,
                     mid_level_inputs1_.size(),
                     LogInputFilesInfo(mid_level_inputs1_).c_str());
    return false;
  }

  if(old_startlevel_inputs_num != start_level_inputs_.size()){
    ROCKS_LOG_BUFFER(log_buffer_, "PickMLOCompaction: action: AfterSetupMidLevelInputs1 "
                                  "start_level: [level: %d , num: %d] %s "
                                  "mid_level1: [level: %d , num: %d] %s "
                                  "result: FAILED "
                                  "reason: ADD_FILES_START-LEVEL-INPUTS",
                                  start_level_inputs_.level,
                                  start_level_inputs_.size(),
                                  LogInputFilesInfo(start_level_inputs_).c_str(),
                                  mid_level_inputs1_.level,
                                  mid_level_inputs1_.size(),
                                  LogInputFilesInfo(mid_level_inputs1_).c_str());
  }
  ROCKS_LOG_BUFFER(log_buffer_, "PickMLOCompaction: action: AfterSetupMidLevelInputs1-BeforeSetupOutputLevelInputs "
                                "start_level: [level: %d , num: %d] %s "
                                "mid_level1: [level: %d , num: %d] %s "
                                "result: SUCCESS ",
                                start_level_inputs_.level,
                                start_level_inputs_.size(),
                                LogInputFilesInfo(start_level_inputs_).c_str(),
                                mid_level_inputs1_.level,
                                mid_level_inputs1_.size(),
                                LogInputFilesInfo(mid_level_inputs1_).c_str());

  size_t old_midinputs1_size = mid_level_inputs1_.size();
  output_level_inputs_.level = mlo_compaction_levels[2];
  if(!compaction_picker_->SetupOtherInputs(
      cf_name_, mutable_cf_options_, vstorage_,
      &mid_level_inputs1_,
      &output_level_inputs_, &parent_index_, parent_index_)){
    ROCKS_LOG_BUFFER(log_buffer_, "PickMLOCompaction: action: AfterSetupOutputLevelInputs "
                                  "mid_level: [level: %d , num: %d] %s "
                                  "output_level1: [level: %d , num: %d] %s "
                                  "result: FAILED "
                     "reason: SETUP-OTHER-INPUTS",
                                  mid_level_inputs1_.level,
                                  mid_level_inputs1_.size(),
                                  LogInputFilesInfo(mid_level_inputs1_).c_str(),
                                  output_level_inputs_.level,
                                  output_level_inputs_.size(),
                                  LogInputFilesInfo(output_level_inputs_).c_str());
    return false;
  }

  if(old_midinputs1_size != mid_level_inputs1_.size()){
    ROCKS_LOG_BUFFER(log_buffer_, "PickMLOCompaction: action: AfterSetupOutputLevelInputs "
                                  "mid_level: [level: %d , num: %d] %s "
                                  "output_level1: [level: %d , num: %d] %s "
                     "result: FAILED "
                     "reason: ADD_FILES_MID-LEVEL-INPUTS ",
                     mid_level_inputs1_.level,
                     mid_level_inputs1_.size(),
                     LogInputFilesInfo(mid_level_inputs1_).c_str(),
                     output_level_inputs_.level,
                     output_level_inputs_.size(),
                     LogInputFilesInfo(output_level_inputs_).c_str());
    return false;
  }else if(output_level_inputs_.size() >=
             (start_level_inputs_.size()+mid_level_inputs1_.size())*3){
    ROCKS_LOG_BUFFER(log_buffer_, "PickMLOCompaction: action: AfterSetupOutputLevelInputs "
                                  "mid_level: [level: %d , num: %d] %s "
                                  "output_level1: [level: %d , num: %d] %s "
                                  "result: FAILED "
                                  "reason: OUTPUT_LEVEL_TOOLARGE ",
                                  mid_level_inputs1_.level,
                                  mid_level_inputs1_.size(),
                                  LogInputFilesInfo(mid_level_inputs1_).c_str(),
                                  output_level_inputs_.level,
                                  output_level_inputs_.size(),
                                  LogInputFilesInfo(output_level_inputs_).c_str());
    return false;
  }

  ROCKS_LOG_BUFFER(log_buffer_, "PickMLOCompaction: action: AfterSetupOutputLevelInputs "
                                "mid_level: [level: %d , num: %d] %s "
                                "output_level1: [level: %d , num: %d] %s "
                                "result: SUCCESS ",
                                mid_level_inputs1_.level,
                                mid_level_inputs1_.size(),
                                LogInputFilesInfo(mid_level_inputs1_).c_str(),
                                output_level_inputs_.level,
                                output_level_inputs_.size(),
                                LogInputFilesInfo(output_level_inputs_).c_str());

  compaction_inputs_.push_back(start_level_inputs_);
  compaction_inputs_.push_back(mid_level_inputs1_);
  if(!output_level_inputs_.empty()){
    compaction_inputs_.push_back(output_level_inputs_);
  }

  for(size_t i = 0; i < compaction_inputs_.size() - 1; i++){
    if(compaction_picker_->FilesRangeOverlapWithCompaction(
            {compaction_inputs_[i],compaction_inputs_[i+1]},
            compaction_inputs_[i+1].level)){
      ROCKS_LOG_BUFFER(log_buffer_,
                       "PickMLOCompaction: action: CheckIfInputsInCompaction "
                       "level: %s "
                       "result: FAILED "
                       "reason: INCOMPACTION ",
                       std::to_string(compaction_inputs_[i+1].level).c_str());
      return false;
    }
  }

  ROCKS_LOG_BUFFER(log_buffer_, "PickMLOCompaction: action: SetupOtherInputs-FINISH "
                                "start_level: [level: %d , num: %d] %s "
                                "mid_level: [level: %d , num: %d] %s "
                                "output_level1: [level: %d , num: %d] %s "
                                "result: SUCCESS ",
                                start_level_inputs_.level,
                                start_level_inputs_.size(),
                                LogInputFilesInfo(start_level_inputs_).c_str(),
                                mid_level_inputs1_.level,
                                mid_level_inputs1_.size(),
                                LogInputFilesInfo(mid_level_inputs1_).c_str(),
                                output_level_inputs_.level,
                                output_level_inputs_.size(),
                                LogInputFilesInfo(output_level_inputs_).c_str());
  return true;
}

Compaction* LevelCompactionBuilder::PickMLOCompaction(){
//  LogCompactionScoreInfo();
  if(!SetupLevelsMLOCompaction()){
    return nullptr;
  }

  SetupInitialFilesMLOCompaction();

  if(start_level_inputs_.empty()){
    ROCKS_LOG_BUFFER(log_buffer_,"PickMLOCompaction: action: AfterSetupInitialFilesMLOCompaction "
                     "result: FAILED "
                     "reason: START_LEVEL_INPUTS_EMPTY");
    return nullptr;
  }

  if(!SetupOtherInputsMLOCompaction()){
    start_level_inputs_.clear();
    output_level_inputs_.clear();
    return nullptr;
  }

  compaction_reason_ = CompactionReason::kLevelMaxLevelSize;
  Compaction* c = GetCompaction();
  c->SetIsMLOCompaction(true);
  c->SetMaxSubCompactions(4);
  return c;
}

//void LevelCompactionBuilder::SetupInitialFilesTrivialMoveCompaction() {
//  for(int i = 1; i < compaction_picker_->NumberLevels() - 1; i++) {
//    double level_score = vstorage_->CompactionLevelScore(i);
//    if(level_score>1){
//      start_level_inputs_.clear();
//      start_level_ = i;
//      if(PickStartLevelFilesTrivialMoveCompaction()){
//        output_level_ = i+1;
//        output_level_inputs_.level = output_level_;
//        break;
//      }
//    }
//  }
//}

//bool LevelCompactionBuilder::PickStartLevelFilesTrivialMoveCompaction() {
//  ROCKS_LOG_BUFFER(log_buffer_, "PickStartLevelFiles TrivialMoveCompaction -Start! ");
//  log_buffer_->FlushBufferToLog();
//  assert(start_level_ > 0);
//  const std::vector<int>& file_size =
//      vstorage_->FilesByCompactionPri(start_level_);
//  const std::vector<FileMetaData*>& level_files =
//      vstorage_->LevelFiles(start_level_);
//  unsigned int cmp_idx;
//  for (cmp_idx = vstorage_->NextCompactionIndex(start_level_);
//  cmp_idx < file_size.size(); cmp_idx++) {
//    int index = file_size[cmp_idx];
//    auto* f = level_files[index];
//    if (f->being_compacted) {
//      continue;
//    }
//
//    start_level_inputs_.files.push_back(f);
//    start_level_inputs_.level = start_level_;
//    size_t old_start_level_input_num = start_level_inputs_.size();
//    if (!compaction_picker_->ExpandInputsToCleanCut(cf_name_, vstorage_,
//                                                    &start_level_inputs_) ||
//                                                    compaction_picker_->FilesRangeOverlapWithCompaction(
//                                                        {start_level_inputs_}, output_level_)) {
//      size_t new_start_level_input_num = start_level_inputs_.size();
//      if(old_start_level_input_num == new_start_level_input_num){
//          start_level_inputs_.files.pop_back();
//      }else{
//        assert(new_start_level_input_num >  old_start_level_input_num);
//        size_t exceed = new_start_level_input_num - old_start_level_input_num + 1;
//        while(exceed--){
//          start_level_inputs_.files.pop_back();
//        }
//      }
//      continue;
//    }
//
//    InternalKey smallest, largest;
//    compaction_picker_->GetRange(start_level_inputs_, &smallest, &largest);
//    CompactionInputFiles output_level_inputs;
//    output_level_inputs.level = start_level_+1;
//    vstorage_->GetOverlappingInputs(start_level_+1, &smallest, &largest,
//                                    &output_level_inputs.files);
//    if (!output_level_inputs.empty() &&
//        ioptions_.compaction_pri == kMinOverlappingRatio) {
//      // only pick files in start_level_ can do trivial move
//      // if files is updated in the order of kMinOverlappingRatio
//      // the following files won't satisfy trivial move
//      size_t new_start_level_input_num = start_level_inputs_.size();
//      if(old_start_level_input_num == new_start_level_input_num){
//        start_level_inputs_.files.pop_back();
//      }else{
//        assert(new_start_level_input_num >  old_start_level_input_num);
//        size_t exceed = new_start_level_input_num - old_start_level_input_num + 1;
//        while(exceed--){
//          start_level_inputs_.files.pop_back();
//        }
//      }
//      break;
//    }
//    base_index_ = index;
//  }
//  if(start_level_inputs_.empty()){
//    ROCKS_LOG_BUFFER(log_buffer_, "PickStartLevelFiles TrivialMoveCompaction - FAILED! "
//                                  "start_level: %d "
//                                  "start_level_input_num: %d ",
//                                  start_level_, start_level_inputs_.size());
//    return false;
//  }
//  vstorage_->SetNextCompactionIndex(start_level_, cmp_idx);
//  ROCKS_LOG_BUFFER(log_buffer_, "PickStartLevelFiles TrivialMoveCompaction - Finish! "
//                   "start_level: %d "
//                   "start_level_input_num: %d ",
//                   start_level_, start_level_inputs_.size());
//  return true;
//}

//Compaction* LevelCompactionBuilder::PickBatchTrivialMoveCompaction() {
//  SetupInitialFilesTrivialMoveCompaction();
//  //no need to setup other inputs in output_level
//  if(start_level_inputs_.empty()){
//    return nullptr;
//  }
//  Compaction* c = GetCompaction();
//  return c;
//}

Compaction* LevelCompactionBuilder::PickCompaction() {
  // Pick up the first file to start compaction. It may have been extended
  // to a clean cut.
  SetupInitialFiles();
  if (start_level_inputs_.empty()) {
    return nullptr;
  }
  assert(start_level_ >= 0 && output_level_ >= 0);

  // If it is a L0 -> base level compaction, we need to set up other L0
  // files if needed.
  if (!SetupOtherL0FilesIfNeeded()) {
    return nullptr;
  }

  // Pick files in the output level and expand more files in the start level
  // if needed.
  if (!SetupOtherInputsIfNeeded()) {
    return nullptr;
  }

  // Form a compaction object containing the files we picked.
  Compaction* c = GetCompaction();

  TEST_SYNC_POINT_CALLBACK("LevelCompactionPicker::PickCompaction:Return", c);

  return c;
}

Compaction* LevelCompactionBuilder::GetCompaction() {
  auto c = new Compaction(
      vstorage_, ioptions_, mutable_cf_options_, mutable_db_options_,
      std::move(compaction_inputs_), output_level_,
      MaxFileSizeForLevel(mutable_cf_options_, output_level_,
                          ioptions_.compaction_style, vstorage_->base_level(),
                          ioptions_.level_compaction_dynamic_level_bytes),
      mutable_cf_options_.max_compaction_bytes,
      GetPathId(ioptions_, mutable_cf_options_, output_level_),
      GetCompressionType(vstorage_, mutable_cf_options_, output_level_,
                         vstorage_->base_level()),
      GetCompressionOptions(mutable_cf_options_, vstorage_, output_level_),
      Temperature::kUnknown,
      /* max_subcompactions */ 0, std::move(grandparents_), is_manual_,
      /* trim_ts */ "", start_level_score_, false /* deletion_compaction */,
      compaction_reason_);

  // If it's level 0 compaction, make sure we don't execute any other level 0
  // compactions in parallel
  compaction_picker_->RegisterCompaction(c);

  // Creating a compaction influences the compaction score because the score
  // takes running compactions into account (by skipping files that are already
  // being compacted). Since we just changed compaction score, we recalculate it
  // here
  vstorage_->ComputeCompactionScore(ioptions_, mutable_cf_options_);
  return c;
}

/*
 * Find the optimal path to place a file
 * Given a level, finds the path where levels up to it will fit in levels
 * up to and including this path
 */
uint32_t LevelCompactionBuilder::GetPathId(
    const ImmutableCFOptions& ioptions,
    const MutableCFOptions& mutable_cf_options, int level) {
  uint32_t p = 0;
  assert(!ioptions.cf_paths.empty());

  // size remaining in the most recent path
  uint64_t current_path_size = ioptions.cf_paths[0].target_size;

  uint64_t level_size;
  int cur_level = 0;

  // max_bytes_for_level_base denotes L1 size.
  // We estimate L0 size to be the same as L1.
  level_size = mutable_cf_options.max_bytes_for_level_base;

  // Last path is the fallback
  while (p < ioptions.cf_paths.size() - 1) {
    if (level_size <= current_path_size) {
      if (cur_level == level) {
        // Does desired level fit in this path?
        return p;
      } else {
        current_path_size -= level_size;
        if (cur_level > 0) {
          if (ioptions.level_compaction_dynamic_level_bytes) {
            // Currently, level_compaction_dynamic_level_bytes is ignored when
            // multiple db paths are specified. https://github.com/facebook/
            // rocksdb/blob/main/db/column_family.cc.
            // Still, adding this check to avoid accidentally using
            // max_bytes_for_level_multiplier_additional
            level_size = static_cast<uint64_t>(
                level_size * mutable_cf_options.max_bytes_for_level_multiplier);
          } else {
            level_size = static_cast<uint64_t>(
                level_size * mutable_cf_options.max_bytes_for_level_multiplier *
                mutable_cf_options.MaxBytesMultiplerAdditional(cur_level));
          }
        }
        cur_level++;
        continue;
      }
    }
    p++;
    current_path_size = ioptions.cf_paths[p].target_size;
  }
  return p;
}

bool LevelCompactionBuilder::PickFileToCompact() {
  // level 0 files are overlapping. So we cannot pick more
  // than one concurrent compactions at this level. This
  // could be made better by looking at key-ranges that are
  // being compacted at level 0.
  if (start_level_ == 0 &&
      !compaction_picker_->level0_compactions_in_progress()->empty()) {
    TEST_SYNC_POINT("LevelCompactionPicker::PickCompactionBySize:0");
    return false;
  }

  start_level_inputs_.clear();
  start_level_inputs_.abandon_outputlevel_toolarge = false;

  assert(start_level_ >= 0);

  // Pick the largest file in this level that is not already
  // being compacted
  const std::vector<int>& file_size =
      vstorage_->FilesByCompactionPri(start_level_);
  const std::vector<FileMetaData*>& level_files =
      vstorage_->LevelFiles(start_level_);

  unsigned int cmp_idx;
  for (cmp_idx = vstorage_->NextCompactionIndex(start_level_);
       cmp_idx < file_size.size(); cmp_idx++) {
    start_level_inputs_.abandon_outputlevel_toolarge = false;
    int index = file_size[cmp_idx];
    auto* f = level_files[index];

    // do not pick a file to compact if it is being compacted
    // from n-1 level.
    if (f->being_compacted) {
      continue;
    }

    start_level_inputs_.files.push_back(f);
    start_level_inputs_.level = start_level_;
    if (!compaction_picker_->ExpandInputsToCleanCut(cf_name_, vstorage_,
                                                    &start_level_inputs_) ||
        compaction_picker_->FilesRangeOverlapWithCompaction(
            {start_level_inputs_}, output_level_)) {
      // A locked (pending compaction) input-level file was pulled in due to
      // user-key overlap.
      start_level_inputs_.clear();
      continue;
    }

    // Now that input level is fully expanded, we check whether any output files
    // are locked due to pending compaction.
    //
    // Note we rely on ExpandInputsToCleanCut() to tell us whether any output-
    // level files are locked, not just the extra ones pulled in for user-key
    // overlap.
    InternalKey smallest, largest;
    compaction_picker_->GetRange(start_level_inputs_, &smallest, &largest);
    CompactionInputFiles output_level_inputs;
    output_level_inputs.level = output_level_;
    vstorage_->GetOverlappingInputs(output_level_, &smallest, &largest,
                                    &output_level_inputs.files);
    if (!output_level_inputs.empty() &&
        !compaction_picker_->ExpandInputsToCleanCut(cf_name_, vstorage_,
                                                    &output_level_inputs)) {
      start_level_inputs_.clear();
      continue;
    }
    //if the total size of output level files involved in compaction is greater than
    // the target level size of output level. We should wait the output level compacted
    if(ioptions_.input_rate_cotroller_enabled){
      uint64_t output_level_target_size = vstorage_->MaxBytesForLevel(output_level_);
      size_t output_level_target_num = (size_t)(output_level_target_size / mutable_cf_options_.target_file_size_base);
      if((output_level_inputs.size() >
           mutable_cf_options_.max_bytes_for_level_multiplier * output_level_target_num) &&
          (output_level_ == start_level_ + 1) &&
          output_level_ == 1){
        ROCKS_LOG_BUFFER(log_buffer_, "CompactionAbandon: output level is too large: "
                         "start_level: %d "
                         "output_level: %d "
                         "output_level_input_num: %d "
                         "output_level_target_num: %d ",
                          start_level_,
                          output_level_,
                          output_level_inputs.size(),
                          output_level_target_num);
        start_level_inputs_.abandon_outputlevel_toolarge = true;
        start_level_inputs_.clear();
        continue;
      }
    }

    base_index_ = index;
    break;
  }

  // store where to start the iteration in the next call to PickCompaction
  vstorage_->SetNextCompactionIndex(start_level_, cmp_idx);

  if(start_level_inputs_.size() > 0 && start_level_==2){
    std::string pri = LogFilesByCompactionPri(start_level_,file_size);
    std::string picked = LogInputFilesInfo(start_level_inputs_);
    ROCKS_LOG_BUFFER(log_buffer_, "PickCompaction: "
                                  "PickedFiles: %s "
                     "FilesByCompactionPri: %s ",
                     picked.c_str(),
                     pri.c_str());
  }

  return start_level_inputs_.size() > 0;
}

bool LevelCompactionBuilder::PickIntraL0Compaction() {
  start_level_inputs_.clear();
  const std::vector<FileMetaData*>& level_files =
      vstorage_->LevelFiles(0 /* level */);
  if (level_files.size() <
          static_cast<size_t>(
              mutable_cf_options_.level0_file_num_compaction_trigger + 2) ||
      level_files[0]->being_compacted) {
    // If L0 isn't accumulating much files beyond the regular trigger, don't
    // resort to L0->L0 compaction yet.
    return false;
  }
  return FindIntraL0Compaction(level_files, kMinFilesForIntraL0Compaction,
                               std::numeric_limits<uint64_t>::max(),
                               mutable_cf_options_.max_compaction_bytes,
                               &start_level_inputs_, earliest_mem_seqno_);
}
}  // namespace

Compaction* LevelCompactionPicker::PickCompaction(
    const std::string& cf_name, const MutableCFOptions& mutable_cf_options,
    const MutableDBOptions& mutable_db_options, VersionStorageInfo* vstorage,
    LogBuffer* log_buffer, SequenceNumber earliest_mem_seqno) {
  LevelCompactionBuilder builder(cf_name, vstorage, earliest_mem_seqno, this,
                                 log_buffer, mutable_cf_options, ioptions_,
                                 mutable_db_options);
  builder.LogCompactionScoreInfo();
//  int ccv = InputRateController::DecideCurDiskWriteStallCondition(builder.vstorage_,builder.mutable_cf_options_);
//  bool dl_ccv = (ccv >> 2) & 1;
//  bool l0_ccv = (ccv >> 1) & 1;
//  Compaction* c = nullptr;
//  if(dl_ccv && ioptions_.input_rate_cotroller_enabled){
//    ROCKS_LOG_BUFFER(log_buffer, "PickMLOCompaction: action: START ");
//    SetIsPickMLOCompaction(true);
//    c = builder.PickMLOCompaction();
//    ROCKS_LOG_BUFFER(log_buffer, "PickMLOCompaction: result: %s ", (c)?"SUCCESS":"FAILED");
//    log_buffer->FlushBufferToLog();
//  }
//  if(!c){
//    SetIsPickMLOCompaction(false);
//    ROCKS_LOG_BUFFER(log_buffer, "PickCompaction: action: START ");
//    c = builder.PickCompaction();
//    if(c && l0_ccv){
//      c->SetMaxSubCompactions(4);
//    }
//    ROCKS_LOG_BUFFER(log_buffer, "PickCompaction: result: %s ", (c)?"SUCCESS":"FAILED");
//  }
//  return c;
  return builder.PickCompaction();
}
}  // namespace ROCKSDB_NAMESPACE
