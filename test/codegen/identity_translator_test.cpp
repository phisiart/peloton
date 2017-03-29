//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// hash_join_translator_test.cpp
//
// Identification: test/codegen/hash_join_translator_test.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/query_compiler.h"
#include "common/harness.h"
#include "concurrency/transaction_manager_factory.h"
#include "planner/identity_plan.h"
#include "planner/seq_scan_plan.h"

#include "codegen/codegen_test_util.h"

namespace peloton {
namespace test {

class IdentityTranslatorTest : public PelotonCodeGenTest {
 public:
  IdentityTranslatorTest() : PelotonCodeGenTest(), num_rows_to_insert(64) {
    LoadTestTable(TestTableId(), num_rows_to_insert);
  }

  uint32_t NumRowsInTestTable() const { return num_rows_to_insert; }

  uint32_t TestTableId() { return test_table1_id; }

 private:
  uint32_t num_rows_to_insert = 64;
};

TEST_F(IdentityTranslatorTest, SimpleTest) {
  //
  // SELECT a, b, c FROM table;
  //

  // Build AST.
  auto seq_scan_plan =
      std::unique_ptr<planner::SeqScanPlan>(new planner::SeqScanPlan(
          &GetTestTable(TestTableId()), nullptr, {0, 1, 2}));

  auto plan = std::unique_ptr<planner::IdentityPlan>(
      new planner::IdentityPlan(std::move(seq_scan_plan)));

  // Semantic analysis.
  planner::BindingContext context;
  plan->PerformBinding(context);

  // Printing consumer
  codegen::BufferingConsumer buffer{{0, 1, 2}, context};

  // COMPILE and execute
  CompileAndExecute(*plan.get(), buffer,
                    reinterpret_cast<char*>(buffer.GetState()));

  // Check that we got all the results
  const auto& results = buffer.GetOutputTuples();
  EXPECT_EQ(results.size(), NumRowsInTestTable());
}

}  // namespace test
}  // namespace peloton
