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

#include "codegen/identity_translator.h"
#include "codegen/compilation_context.h"
#include "codegen/operator_translator.h"

namespace peloton {
namespace codegen {

IdentityTranslator::IdentityTranslator(const planner::IdentityPlan &plan,
                                       CompilationContext &context,
                                       Pipeline &pipeline)
    : OperatorTranslator(context, pipeline), identity_plan_(plan) {
  context.Prepare(*plan.GetChild(0), pipeline);
}

void IdentityTranslator::Produce() const {
  this->GetCompilationContext().Produce(*this->identity_plan_.GetChild(0));
}

void IdentityTranslator::Consume(ConsumerContext &context,
                                 RowBatch &batch) const {
  // Push the batch into the pipeline
  //  ConsumerContext context{translator_.GetCompilationContext(),
  //                          translator_.GetPipeline()};
  context.Consume(batch);
}

void IdentityTranslator::Consume(ConsumerContext &context,
                                 RowBatch::Row &row) const {
  // Push the batch into the pipeline
  //  ConsumerContext context{translator_.GetCompilationContext(),
  //                          translator_.GetPipeline()};
  context.Consume(row);
}

}  // namespace codegen
}  // namespace peloton
