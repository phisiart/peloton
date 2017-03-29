//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// negation_translator.h
//
// Identification: src/include/codegen/negation_translator.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include "codegen/compilation_context.h"
#include "codegen/operator_translator.h"
#include "planner/identity_plan.h"

namespace peloton {
namespace codegen {

class IdentityTranslator : public OperatorTranslator {
 public:
  IdentityTranslator(const planner::IdentityPlan &plan,
                     CompilationContext &context, Pipeline &pipeline);

  ~IdentityTranslator() override {}

  // Don't need any state
  void InitializeState() override {}

  // Don't need any state
  void TearDownState() override {}

  // Don't need any auxiliary function
  void DefineAuxiliaryFunctions() override {}

  // Get a stringified version of this translator
  std::string GetName() const override { return "identity"; }

  // Plan accessor
  const planner::IdentityPlan &GetIdentityPlan() const {
    return identity_plan_;
  }

  // ============================================
  // The following member functions are critical.
  // ============================================

  // The method that produces new tuples
  void Produce() const override;

  // The method that consumers tuples produced by the child
  void Consume(ConsumerContext &, RowBatch &) const override;
  void Consume(ConsumerContext &, RowBatch::Row &) const override;

 private:
  // The identity plan
  const planner::IdentityPlan &identity_plan_;
};

}  // namespace codegen
}  // namespace peloton
