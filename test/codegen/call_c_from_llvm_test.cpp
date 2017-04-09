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

// Step 1. Give a function prototype.
// Let's explicitly specify "extern C".
// This avoids any name mangling and other confusing stuff.
// The down side of doing this is that, you don't have
// - Fancy arguments (C++ reference, etc.)
// - Templates
// - Class member functions (obj->method(args))
// - etc.
extern "C" {
  int my_c_library_add1(int x);
}

// Step 2. Implement the function.
// Note that in the function implementation, you can use C++.
// It's just that you can't use C++ on the function prototype.
int my_c_library_add1(int x) { // This line is plain C.

  // Now we are free to use C++.
  class MyFancyAdder {
   public:
    MyFancyAdder(int val) : val_(val) {}
    int operator+(int other) { return this->val_ + other; }
   private:
    int val_;
  };

  return MyFancyAdder(x) + 1;
}

namespace peloton {
namespace codegen {

// Step 3. Let's wrap up the C function with a proxy.
// This let's us call this function in LLVM more easily.
class MyCLibraryProxy {
 public:
  struct _Add1 {
    static std::string GetFunctionName() {
      return "my_c_library_add1";
    }

    static llvm::Function *GetFunction(CodeGen &codegen) {
      const std::string fn_name = GetFunctionName();

      llvm::Function* fn = codegen.LookupFunction(fn_name);
      if (fn != nullptr) {
        return fn;
      }

      // Create a function type:
      // int f(int)
      llvm::FunctionType *fn_type =
          llvm::FunctionType::get(codegen.Int32Type(), codegen.Int32Type());

      // Register a function with "external linkage".
      // This is basically giving LLVM a "function prototype".
      return codegen.RegisterFunction(fn_name, fn_type);
    }

  };  // struct _Add1
};  // class MyCLibraryProxy

}  // namespace codegen
}  // namespace peloton

namespace peloton {
namespace test {

class CallCFromLlvmTest : public PelotonTest {};

TEST_F(CallCFromLlvmTest, SimpleCFunction) {
  codegen::CodeContext code_context;
  codegen::CodeGen cg{code_context};

  codegen::FunctionBuilder func{code_context, "test_func", cg.Int32Type(), {}};
  {
    auto ret = cg.CallFunc(
        peloton::codegen::MyCLibraryProxy::_Add1::GetFunction(cg),
        { cg.Const32(100) }
    );

    func.ReturnAndFinish(ret);
  }

  ASSERT_TRUE(code_context.Compile());

  typedef int (*func_t)(void);
  func_t test_func = (func_t)code_context.GetFunctionPointer(func.GetFunction());
  ASSERT_EQ(test_func(), 101);
}

}  // namespace test
}  // namespace peloton
