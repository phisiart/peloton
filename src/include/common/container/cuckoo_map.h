//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// cuckoo_map.h
//
// Identification: src/include/container/cuckoo_map.h
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//


#pragma once

#include <cstdlib>
#include <cstring>
#include <cstdio>

#include "libcuckoo/cuckoohash_map.hh"

namespace peloton {

#define CUCKOO_MAP_TEMPLATE_ARGUMENTS                                          \
    template <typename KeyType, typename ValueType,                            \
              typename Hash>

#define CUCKOO_MAP_TEMPLATE_ARGUMENTS_DEFINITION                               \
    template <typename KeyType, typename ValueType,                            \
              typename Hash = DefaultHasher<KeyType>>

#define CUCKOO_MAP_TYPE CuckooMap<KeyType, ValueType, Hash>

CUCKOO_MAP_TEMPLATE_ARGUMENTS_DEFINITION
class CuckooMap {
 public:

  CuckooMap();
  ~CuckooMap();

  // Inserts a item
  bool Insert(const KeyType &key, ValueType value);

  // Extracts item with high priority
  bool Update(const KeyType &key, ValueType value);

  // Extracts the corresponding value
  bool Find(const KeyType &key, ValueType &value) const;

  // Delete key from the cuckoo_map
  bool Erase(const KeyType &key);

  // Checks whether the cuckoo_map contains key
  bool Contains(const KeyType &key);

  // Clears the tree (thread safe, not atomic)
  void Clear();

  // Returns item count in the cuckoo_map
  size_t GetSize() const;

  // Checks if the cuckoo_map is empty
  bool IsEmpty() const;

  void Upsert(const KeyType &key, ValueType value,
              std::function<void(ValueType&)> updater);

  void ForEach(std::function<void(const KeyType &, const ValueType &)> func);

  void ForLoop(std::function<bool(const KeyType &, const ValueType &)> func);

 private:

  // cuckoo map
  typedef cuckoohash_map<KeyType, ValueType, Hash> cuckoo_map_t;

  cuckoo_map_t cuckoo_map;
};

}  // namespace peloton
