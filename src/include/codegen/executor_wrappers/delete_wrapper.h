//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// aggregation.h
//
// Identification: src/include/codegen/aggregation.h
//
// Copyright (c) 2015-17, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <stdint.h>

namespace peloton {
namespace codegen {
    class DeleteWrapper {
    public:
      uint32_t someWeirdFunction();
      static bool DeleteWrapper::DExecute();

    };  // class DeleteWrapper

}  // namespace codegen
}  // namespace peloton
