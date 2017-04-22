//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// hash_group_by_translator.cpp
//
// Identification: src/codegen/insert/insert_scan_translator.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/raw_tuple/raw_tuple_runtime_proxy.h"
#include "codegen/insert/insert_helpers_proxy.h"
#include "codegen/schema/schema_proxy.h"
#include "codegen/catalog_proxy.h"
#include "codegen/data_table_proxy.h"
#include "planner/abstract_scan_plan.h"
#include "storage/data_table.h"
#include "codegen/insert/insert_scan_translator.h"
#include "codegen/raw_tuple/raw_tuple_ref.h"

namespace peloton {
namespace codegen {

InsertScanTranslator::InsertScanTranslator(const planner::InsertPlan &insert_plan,
                                           CompilationContext &context,
                                           Pipeline &pipeline)
    : AbstractInsertTranslator(insert_plan, context, pipeline) {

  // Also create the translator for our child.
  context.Prepare(*insert_plan.GetChild(0), pipeline);

  std::unique_ptr<storage::Tuple> tuple(new storage::Tuple(
      insert_plan.GetTable()->GetSchema(), true));

  this->pool_ = std::make_unique<type::EphemeralPool>();
  this->tuple_data_ = tuple->GetData();
}

void InsertScanTranslator::Produce() const {
  auto &compilation_context = this->GetCompilationContext();

  auto &codegen = this->GetCodeGen();
  storage::DataTable *table = this->insert_plan_.GetTable();

  llvm::Value *table_ptr = codegen.CallFunc(
      CatalogProxy::_GetTableWithOid::GetFunction(codegen),
      {
          GetCatalogPtr(),
          codegen.Const32(table->GetDatabaseOid()),
          codegen.Const32(table->GetOid())
      }
  );
  (void)table_ptr;

  catalog::Schema *schema = table->GetSchema();

  LOG_DEBUG("schema = %p", schema);

  llvm::Value *schema_ptr = codegen.Const64(reinterpret_cast<int64_t>(schema));
  schema_ptr = codegen->CreateBitOrPointerCast(
      schema_ptr, SchemaProxy::GetType(codegen)->getPointerTo());

//  llvm::Value *schema_ptr = codegen.CallFunc(
//      DataTableProxy::_GetSchema::GetFunction(codegen),
//      { table_ptr }
//  );

  llvm::Value *tuple_ptr = codegen.CallFunc(
      InsertHelpersProxy::_CreateTuple::GetFunction(codegen),
      { schema_ptr }
  );

  llvm::Value *tuple_data_ptr = codegen.CallFunc(
      InsertHelpersProxy::_GetTupleData::GetFunction(codegen),
      { tuple_ptr }
  );

  this->tuple_ptr_ = tuple_ptr;
  this->tuple_data_ptr_ = tuple_data_ptr;

//  auto &runtime_state = compilation_context.GetRuntimeState();
//  RuntimeState::StateID tuple_ptr_state_id = runtime_state.RegisterState(
//      "insert_tuple_ptr",
//      codegen.Int8Type()->getPointerTo()
//  );
//  RuntimeState::StateID tuple_data_ptr_state_id = runtime_state.RegisterState(
//      "insert_tuple_data_ptr",
//      codegen.Int8Type()->getPointerTo()
//  );
//  runtime_state.LoadStatePtr(codegen, tuple_ptr_state_id);
//  runtime_state.LoadStatePtr(codegen, tuple_data_ptr_state_id);

  // the child of delete executor will be a scan. call produce function
  // of the child to produce the scanning result
  compilation_context.Produce(*insert_plan_.GetChild(0));
}

void InsertScanTranslator::Consume(ConsumerContext &context,
                                   RowBatch::Row &row) const {

  storage::DataTable *table = this->insert_plan_.GetTable();
  catalog::Schema *schema = table->GetSchema();
  auto ncolumns = schema->GetColumnCount();
  auto &codegen = this->GetCodeGen();

  llvm::Value *tile_group_id = row.GetTileGroupID();
  llvm::Value *tuple_id = row.GetTID(this->GetCodeGen());

  this->GetCodeGen().CallPrintf(
      "tile_group_id: %d, tuple_id: %d\n",
      { tile_group_id, tuple_id }
  );

  std::vector<const planner::AttributeInfo *> ais;

  auto scan = static_cast<const planner::AbstractScan *>(
      this->insert_plan_.GetChild(0));

  scan->GetAttributes(ais);

  RawTupleRef raw_tuple_ref(codegen, row, schema, ais, this->tuple_data_ptr_);

  for (oid_t i = 0; i != ncolumns; ++i) {
    raw_tuple_ref.Materialize(i);
  }

  codegen.CallFunc(
      RawTupleRuntimeProxy::_DumpTuple::GetFunction(codegen),
      { this->tuple_ptr_ }
  );

  llvm::Value *catalog_ptr = GetCatalogPtr();

  llvm::Value *txn_ptr = GetCompilationContext().GetTransactionPtr();

  llvm::Value *table_ptr =
      codegen.CallFunc(CatalogProxy::_GetTableWithOid::GetFunction(codegen),
                       {catalog_ptr, codegen.Const32(table->GetDatabaseOid()),
                        codegen.Const32(table->GetOid())});

  codegen.CallFunc(
      InsertHelpersProxy::_InsertRawTuple::GetFunction(codegen),
      { txn_ptr, table_ptr, this->tuple_ptr_ }
  );

  (void)context;
  (void)row;
  // @todo Implement this.
}

void InsertScanTranslator::Consume(ConsumerContext &context,
                                   RowBatch &batch) const {
  OperatorTranslator::Consume(context, batch);
}

}  // namespace codegen
}  // namespace peloton
