//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// aggregate_plan.h
//
// Identification: src/include/planner/aggregate_plan.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "planner/abstract_plan.h"

namespace peloton {
namespace planner {

// This plan simply passes out whatever output its child produces.
// I wrote this plan as an exercise.
class IdentityPlan : public AbstractPlan {
 public:
  IdentityPlan(std::unique_ptr<AbstractPlan> &&plan) {
    this->AddChild(std::move(plan));
  }

  PlanNodeType GetPlanNodeType() const override {
    return PlanNodeType::IDENTITY;
  }

  std::unique_ptr<AbstractPlan> Copy() const override {
    return std::unique_ptr<AbstractPlan>(
        new IdentityPlan(this->GetChild(0)->Copy()));
  }
};

}  // namespace planner
}  // namespace peloton
