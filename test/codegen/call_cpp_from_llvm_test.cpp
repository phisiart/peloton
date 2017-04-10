//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// function_builder_test.cpp
//
// Identification: test/codegen/function_builder_test.cpp
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "codegen/codegen.h"
#include "codegen/runtime_functions_proxy.h"
#include "common/harness.h"
#include "codegen/executor_wrappers/delete_wrapper.h"

namespace peloton {
namespace test {

  class CallCPPFromLlvmTest : public PelotonTest {};

  TEST_F(CallCPPFromLlvmTest, SimpleCPPFunction) {
    codegen::CodeContext code_context;
    codegen::CodeGen cg{code_context};

    // mingled name for peloton::codegen::DeleteWrapper::someWeirdFunction()
    const std::string fn_name = "_ZN7peloton7codegen13DeleteWrapper17someWeirdFunctionEv";

    llvm::Function* fn = cg.LookupFunction(fn_name);
    if (fn == nullptr) {
      llvm::FunctionType *fn_type = llvm::FunctionType::get(cg.Int32Type(), false);
      fn = cg.RegisterFunction(fn_name, fn_type);
    }

    codegen::FunctionBuilder func{code_context, "test_func", cg.Int32Type(), {}};
    {
      auto ret = cg.CallFunc(fn, { });
      func.ReturnAndFinish(ret);
    }
    ASSERT_TRUE(code_context.Compile());

    typedef int (*func_t)(void);
    func_t test_func = (func_t)code_context.GetFunctionPointer(func.GetFunction());
    ASSERT_EQ(test_func(), 123454321);
  }

}  // namespace test
}  // namespace peloton
