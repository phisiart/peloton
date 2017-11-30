//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// task_info.cpp
//
// Identification: src/codegen/multi_thread/multithread_supported.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "planner/hash_join_plan.h"
#include "planner/hash_plan.h"
#include "codegen/multi_thread/multithread_supported.h"

namespace peloton {
namespace codegen {

bool MultithreadSupported(const planner::AbstractPlan &plan) {
  switch (plan.GetPlanNodeType()) {
// Temporarily turned off...
    case PlanNodeType::SEQSCAN: {
      return true;
    }
    case PlanNodeType::HASHJOIN: {
      auto& hash_join_plan = dynamic_cast<const planner::HashJoinPlan&>(plan);
      for (auto& child : hash_join_plan.GetChildren()) {
        if (!MultithreadSupported(*child)) {
          return false;
        }
      }
      return true;
    }
    case PlanNodeType::HASH: {
      auto& hash_join_plan = dynamic_cast<const planner::HashPlan&>(plan);
      for (auto& child : hash_join_plan.GetChildren()) {
        if (!MultithreadSupported(*child)) {
          return false;
        }
      }
      return true;
    }
    default:
      return false;
  }
}

}  // namespace codegen
}  // namespace peloton
