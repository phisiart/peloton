//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// task_info.cpp
//
// Identification: src/codegen/multi_thread/task_info.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/multi_thread/task_info.h"

namespace peloton {
namespace codegen {

void TaskInfo::Init(int32_t thread_id, int32_t nthreads) {
  PL_ASSERT(thread_id > 0 && thread_id < nthreads);
  thread_id_ = thread_id;
  nthreads_ = nthreads;
}

int32_t TaskInfo::GetThreadId() {
  return thread_id_;
}

int32_t TaskInfo::GetNumThreads() {
  return nthreads_;
}

}  // namespace codegen
}  // namespace peloton