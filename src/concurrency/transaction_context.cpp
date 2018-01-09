//===----------------------------------------------------------------------===//
//
//                         Peloton
//
// transaction_context.cpp
//
// Identification: src/concurrency/transaction_context.cpp
//
// Copyright (c) 2015-16, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "concurrency/transaction_context.h"

#include <sstream>

#include "common/logger.h"
#include "common/platform.h"
#include "common/macros.h"
#include "trigger/trigger.h"

#include <chrono>
#include <thread>
#include <iomanip>

namespace peloton {
namespace concurrency {

/*
 * TransactionContext state transition:
 *                r           r/ro            u/r/ro
 *              +--<--+     +---<--+        +---<--+
 *           r  |     |     |      |        |      |     d
 *  (init)-->-- +-> Read  --+-> Read Own ---+--> Update ---> Delete (final)
 *                    |   ro             u  |
 *                    |                     |
 *                    +----->--------->-----+
 *                              u
 *              r/ro/u
 *            +---<---+
 *         i  |       |     d
 *  (init)-->-+---> Insert ---> Ins_Del (final)
 *
 *    r : read
 *    ro: read_own
 *    u : update
 *    d : delete
 *    i : insert
 */

TransactionContext::TransactionContext(const size_t thread_id,
                         const IsolationLevelType isolation,
                         const cid_t &read_id) {
  Init(thread_id, isolation, read_id);
}

TransactionContext::TransactionContext(const size_t thread_id,
                         const IsolationLevelType isolation,
                         const cid_t &read_id, const cid_t &commit_id) {
  Init(thread_id, isolation, read_id, commit_id);
}

TransactionContext::~TransactionContext() {}

void TransactionContext::Init(const size_t thread_id,
                       const IsolationLevelType isolation, const cid_t &read_id,
                       const cid_t &commit_id) {
  read_id_ = read_id;

  // commit id can be set at a transaction's commit phase.
  commit_id_ = commit_id;

  // set txn_id to commit_id.
  txn_id_ = commit_id_;

  epoch_id_ = read_id_ >> 32;

  thread_id_ = thread_id;

  isolation_level_ = isolation;

  is_written_ = false;

  insert_count_ = 0;

  gc_set_.reset(new GCSet());
  gc_object_set_.reset(new GCObjectSet());

  on_commit_triggers_.reset();
}

RWType TransactionContext::GetRWType(const ItemPointer &location) {
  RWType type;
  if (rw_set_.Find(location, type)) {
    return type;
  } else {
    return RWType::INVALID;
  }
}

void TransactionContext::RecordRead(const ItemPointer &location) {
  rw_set_.Upsert(location, RWType::READ, [](RWType &type) {
    switch (type) {
      case RWType::INVALID:
      case RWType::DELETE:
      case RWType::INS_DEL: {
        PL_ASSERT(false && "Bad RWType");
        break;
      }
      default: {
        break;
      }
    }
  });
}

void TransactionContext::RecordReadOwn(const ItemPointer &location) {
  rw_set_.Upsert(location, RWType::READ_OWN, [](RWType &type) {
    switch (type) {
      case RWType::READ: {
        type = RWType::READ_OWN;
        break;
      }
      case RWType::READ_OWN:
      case RWType::UPDATE:
      case RWType::INSERT: {
        break;
      }
      case RWType::INVALID:
      case RWType::DELETE:
      case RWType::INS_DEL: {
        PL_ASSERT(false && "Bad RWType");
        break;
      }
    }
  });
}

void TransactionContext::RecordUpdate(const ItemPointer &location) {
  rw_set_.Upsert(location, RWType::UPDATE, [this](RWType &type) {
    switch (type) {
      case RWType::READ:
      case RWType::READ_OWN: {
        type = RWType::UPDATE;
        is_written_ = true;  // Not synchronized, but it is okay.
        break;
      }
      case RWType::UPDATE:
      case RWType::INSERT: {
        break;
      }
      case RWType::INVALID:
      case RWType::DELETE:
      case RWType::INS_DEL: {
        PL_ASSERT(false && "Bad RWType");
        break;
      }
    }
  });
}

void TransactionContext::RecordInsert(const ItemPointer &location) {
  rw_set_.Upsert(location, RWType::INSERT, [](RWType &) {
    PL_ASSERT(false && "Bad RWType");
  });
  ++insert_count_;
}

bool TransactionContext::RecordDelete(const ItemPointer &location) {
  bool ret = false;
  rw_set_.Upsert(location, RWType::DELETE, [this, &ret](RWType &type) {
    switch (type) {
      case RWType::READ:
      case RWType::READ_OWN: {
        type = RWType::DELETE;
        is_written_ = true;
        ret = false;
        break;
      }
      case RWType::UPDATE: {
        type = RWType::DELETE;
        ret = false;
        break;
      }
      case RWType::INSERT: {
        type = RWType::INS_DEL;
        --insert_count_;
        ret = true;
        break;
      }
      case RWType::INVALID:
      case RWType::DELETE:
      case RWType::INS_DEL: {
        PL_ASSERT(false && "Bad RWType");
        break;
      }
    }
  });
  return ret;
}

const std::string TransactionContext::GetInfo() const {
  std::ostringstream os;

  os << " Txn :: @" << this << " ID : " << std::setw(4) << txn_id_
     << " Read ID : " << std::setw(4) << read_id_
     << " Commit ID : " << std::setw(4) << commit_id_
     << " Result : " << result_;

  return os.str();
}

void TransactionContext::AddOnCommitTrigger(trigger::TriggerData &trigger_data) {
  if (on_commit_triggers_ == nullptr) {
    on_commit_triggers_.reset(new trigger::TriggerSet());
  }
  on_commit_triggers_->push_back(trigger_data);
}

void TransactionContext::ExecOnCommitTriggers() {
  if (on_commit_triggers_ != nullptr) {
    on_commit_triggers_->ExecTriggers();
  }
}

}  // namespace concurrency
}  // namespace peloton
