// Copyright (c) 2023 dingodb.com, Inc. All Rights Reserved
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "engine/txn_engine_helper.h"

#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <string_view>
#include <vector>

#include "butil/status.h"
#include "common/constant.h"
#include "common/helper.h"
#include "common/logging.h"
#include "engine/raw_engine.h"
#include "fmt/core.h"
#include "proto/store.pb.h"

namespace dingodb {

DECLARE_uint32(max_short_value_in_write_cf);

butil::Status TxnEngineHelper::GetLockInfo(const std::shared_ptr<RawEngine::Reader> &reader, const std::string &key,
                                           pb::store::LockInfo &lock_info) {
  std::string lock_value;
  auto status = reader->KvGet(Helper::EncodeTxnKey(key, Constant::kLockVer), lock_value);
  // if lock_value is not found or it is empty, then the key is not locked
  // else the key is locked, return WriteConflict
  if (status.error_code() == pb::error::Errno::EKEY_NOT_FOUND) {
    // key is not exists, the key is not locked
    DINGO_LOG(INFO) << "[txn]GetLockInfo key: " << key << " is not locked, lock_key is not exist";
    return butil::Status::OK();
  }

  if (!status.ok()) {
    // other error, return error
    DINGO_LOG(ERROR) << "[txn]GetLockInfo read lock_key failed, lock_key: " << key
                     << ", status: " << status.error_str();
    return butil::Status(status.error_code(), status.error_str());
  }

  if (lock_value.empty()) {
    // lock_value is empty, the key is not locked
    DINGO_LOG(INFO) << "[txn]GetLockInfo key: " << key << " is not locked, lock_value is null";
    return butil::Status::OK();
  }

  auto ret = lock_info.ParseFromString(lock_value);
  if (!ret) {
    DINGO_LOG(FATAL) << "[txn]GetLockInfo parse lock info failed, lock_key: " << key
                     << ", lock_value(hex): " << Helper::StringToHex(lock_value);
  }

  return butil::Status::OK();
}

butil::Status TxnEngineHelper::ScanLockInfo(const std::shared_ptr<RawEngine> &engine, uint64_t min_lock_ts,
                                            uint64_t max_lock_ts, const std::string &start_key,
                                            const std::string &end_key, uint32_t limit,
                                            std::vector<pb::store::LockInfo> &lock_infos) {
  IteratorOptions iter_options;
  iter_options.lower_bound = Helper::EncodeTxnKey(start_key, Constant::kLockVer);
  iter_options.upper_bound = Helper::EncodeTxnKey(end_key, Constant::kLockVer);

  auto iter = engine->NewIterator(Constant::kTxnLockCF, iter_options);
  if (iter == nullptr) {
    DINGO_LOG(FATAL) << "[txn]GetLockInfo NewIterator failed, start_key: " << start_key << ", end_key: " << end_key;
  }

  iter->SeekToFirst();
  while (iter->Valid()) {
    auto lock_value = iter->Value();
    if (lock_value.length() <= 8) {
      DINGO_LOG(FATAL) << "invalid lock_value, key: " << Helper::StringToHex(iter->Key())
                       << ", min_lock_ts: " << min_lock_ts
                       << ", lock_value is less than 8 bytes: " << Helper::StringToHex(lock_value);
    }

    pb::store::LockInfo lock_info;
    auto ret = lock_info.ParseFromArray(lock_value.data(), lock_value.size());
    if (!ret) {
      DINGO_LOG(FATAL)
      "parse lock info failed, key: " << Helper::StringToHex(iter->Key())
                                      << ", lock_value(hex): " << Helper::StringToHex(lock_value);
    }

    DINGO_LOG(INFO) << "get lock_info lock_ts: " << lock_info.lock_ts()
                    << ", lock_info: " << lock_info.ShortDebugString()
                    << ", iter->key: " << Helper::StringToHex(iter->Key())
                    << ", lock_key: " << Helper::StringToHex(lock_info.key());

    // if lock is not exist, nothing to do
    if (lock_info.lock_ts() == 0) {
      DINGO_LOG(WARNING) << "txn_not_found with lock_info empty, iter->key: " << Helper::StringToHex(iter->Key());

      // auto *txn_not_found = txn_result->mutable_txn_not_found();
      // txn_not_found->set_start_ts(start_ts);
      iter->Next();
      continue;
    }

    if (lock_info.lock_ts() < min_lock_ts || lock_info.lock_ts() >= max_lock_ts) {
      DINGO_LOG(WARNING) << "txn_not_found with lock_info.lock_ts not in range, iter->key: "
                         << Helper::StringToHex(iter->Key()) << ", min_lock_ts: " << min_lock_ts
                         << ", max_lock_ts: " << max_lock_ts << ", lock_info: " << lock_info.ShortDebugString();
      iter->Next();
      continue;
    }

    lock_infos.push_back(lock_info);

    if (limit > 0 && lock_infos.size() >= limit) {
      break;
    }

    iter->Next();
  }

  return butil::Status::OK();
}

// Rollback
// This function is not saft, MUST be called in raft apply to make sure the lock_info is not changed during rollback
butil::Status TxnEngineHelper::Rollback(const std::shared_ptr<RawEngine> &engine,
                                        std::vector<std::string> &keys_to_rollback_with_data,
                                        std::vector<std::string> &keys_to_rollback_without_data, uint64_t start_ts) {
  DINGO_LOG(INFO) << "[txn]Rollback start_ts: " << start_ts
                  << ", keys_count_with_data: " << keys_to_rollback_with_data.size()
                  << ", keys_count_without_data: " << keys_to_rollback_without_data.size();

  std::vector<pb::common::KeyValue> kv_puts_write;
  std::vector<std::string> kv_deletes_lock;
  std::vector<std::string> kv_deletes_data;

  for (const auto &key : keys_to_rollback_without_data) {
    // delete lock
    kv_deletes_lock.emplace_back(Helper::EncodeTxnKey(key, Constant::kLockVer));

    // delete write
    pb::store::WriteInfo write_info;
    write_info.set_start_ts(start_ts);
    write_info.set_op(::dingodb::pb::store::Op::Rollback);

    pb::common::KeyValue kv;
    kv.set_key(Helper::EncodeTxnKey(key, start_ts));
    kv.set_value(write_info.SerializeAsString());
    kv_puts_write.emplace_back(kv);
  }

  for (const auto &key : keys_to_rollback_without_data) {
    // delete lock
    kv_deletes_lock.emplace_back(Helper::EncodeTxnKey(key, Constant::kLockVer));

    // delete data
    kv_deletes_data.emplace_back(Helper::EncodeTxnKey(key, start_ts));

    // delete write
    pb::store::WriteInfo write_info;
    write_info.set_start_ts(start_ts);
    write_info.set_op(::dingodb::pb::store::Op::Rollback);

    pb::common::KeyValue kv;
    kv.set_key(Helper::EncodeTxnKey(key, start_ts));
    kv.set_value(write_info.SerializeAsString());
    kv_puts_write.emplace_back(kv);
  }

  // after all mutations is processed, write into raw engine
  std::map<uint32_t, std::vector<pb::common::KeyValue>> kv_puts_with_cf;
  std::map<uint32_t, std::vector<std::string>> kv_deletes_with_cf;

  kv_puts_with_cf.insert_or_assign(Constant::kTxnWriteCfId, kv_puts_write);
  kv_deletes_with_cf.insert_or_assign(Constant::kTxnLockCfId, kv_deletes_lock);
  kv_deletes_with_cf.insert_or_assign(Constant::kTxnDataCfId, kv_deletes_data);

  auto writer = engine->NewMultiCfWriter(Helper::GenMvccCfVector());
  if (writer == nullptr) {
    DINGO_LOG(FATAL) << "[txn]Rollback NewMultiCfWriter failed, start_ts: " << start_ts;
  }

  auto status = writer->KvBatchPutAndDelete(kv_puts_with_cf, kv_deletes_with_cf);
  if (!status.ok()) {
    DINGO_LOG(FATAL) << "[txn]Rollback KvBatchPutAndDelete failed, start_ts: " << start_ts
                     << ", status: " << status.error_str();
  }

  return butil::Status::OK();
}

// Commit (deprecated and only can be used for table region)
// This function is not saft, MUST be called in raft apply to make sure the lock_info is not changed during commit
butil::Status TxnEngineHelper::Commit(const std::shared_ptr<RawEngine> &engine,
                                      std::vector<pb::store::LockInfo> &lock_infos, uint64_t commit_ts) {
  DINGO_LOG(INFO) << "[txn]Commit commit_ts: " << commit_ts << ", keys_count: " << lock_infos.size()
                  << ", first_key: " << Helper::StringToHex(lock_infos[0].key())
                  << ", first_lock_ts: " << lock_infos[0].lock_ts()
                  << ", last_key: " << Helper::StringToHex(lock_infos[lock_infos.size() - 1].key());

  std::vector<pb::common::KeyValue> kv_puts_write;
  std::vector<std::string> kv_deletes_lock;

  auto data_reader = engine->NewReader(Constant::kTxnDataCF);
  if (data_reader == nullptr) {
    DINGO_LOG(FATAL) << "[txn]Commit NewReader failed, commit_ts: " << commit_ts;
  }

  for (const auto &lock_info : lock_infos) {
    // now txn is match, prepare to commit
    // 1.put data to write_cf
    std::string data_value;
    if (lock_info.short_value().length() > 0) {
      data_value = lock_info.short_value();
    } else if (lock_info.lock_type() == pb::store::Put) {
      auto ret = data_reader->KvGet(Helper::EncodeTxnKey(lock_info.key(), lock_info.lock_ts()), data_value);
      if (!ret.ok() && ret.error_code() != pb::error::Errno::EKEY_NOT_FOUND) {
        DINGO_LOG(FATAL) << "[txn]Commit read data failed, key: " << lock_info.key() << ", status: " << ret.error_str();
      }
    }

    {
      pb::common::KeyValue kv;
      std::string write_key = Helper::EncodeTxnKey(lock_info.key(), commit_ts);
      kv.set_key(write_key);

      pb::store::WriteInfo write_info;
      write_info.set_start_ts(lock_info.lock_ts());
      write_info.set_op(lock_info.lock_type());
      if (!data_value.empty()) {
        write_info.set_short_value(data_value);
      }
      kv.set_value(write_info.SerializeAsString());

      kv_puts_write.push_back(kv);
    }

    // 3.delete lock from lock_cf
    { kv_deletes_lock.push_back(Helper::EncodeTxnKey(lock_info.key(), Constant::kLockVer)); }
  }

  // after all mutations is processed, write into raw engine
  std::map<uint32_t, std::vector<pb::common::KeyValue>> kv_puts_with_cf;
  std::map<uint32_t, std::vector<std::string>> kv_deletes_with_cf;

  kv_puts_with_cf.insert_or_assign(Constant::kTxnWriteCfId, kv_puts_write);
  kv_deletes_with_cf.insert_or_assign(Constant::kTxnLockCfId, kv_deletes_lock);

  auto writer = engine->NewMultiCfWriter(Helper::GenMvccCfVector());
  if (writer == nullptr) {
    DINGO_LOG(FATAL) << "[txn]Commit NewMultiCfWriter failed, commit_ts: " << commit_ts;
  }

  auto status = writer->KvBatchPutAndDelete(kv_puts_with_cf, kv_deletes_with_cf);
  if (!status.ok()) {
    DINGO_LOG(FATAL) << "[txn]Commit KvBatchPutAndDelete failed, commit_ts: " << commit_ts
                     << ", status: " << status.error_str();
  }

  return butil::Status::OK();
}

butil::Status TxnEngineHelper::BatchGet(const std::shared_ptr<RawEngine> &engine,
                                        const pb::store::IsolationLevel &isolation_level, uint64_t start_ts,
                                        const std::vector<std::string> &keys, std::vector<pb::common::KeyValue> &kvs,
                                        pb::store::TxnResultInfo &txn_result_info) {
  DINGO_LOG(INFO) << "[txn]BatchGet keys_count: " << keys.size() << ", isolation_level: " << isolation_level
                  << ", start_ts: " << start_ts << ", first_key: " << Helper::StringToHex(keys[0])
                  << ", last_key: " << Helper::StringToHex(keys[keys.size() - 1]);

  if (keys.empty()) {
    return butil::Status::OK();
  }

  if (engine == nullptr) {
    DINGO_LOG(FATAL) << "[txn]BatchGet engine is null";
  }

  if (!kvs.empty()) {
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "kvs is not empty");
  }

  if (txn_result_info.ByteSizeLong() > 0) {
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "txn_result_info is not empty");
  }

  auto lock_reader = engine->NewReader(Constant::kTxnLockCF);
  if (lock_reader == nullptr) {
    DINGO_LOG(FATAL) << "[txn]BatchGet NewReader failed, start_ts: " << start_ts;
  }

  auto data_reader = engine->NewReader(Constant::kTxnDataCF);
  if (data_reader == nullptr) {
    DINGO_LOG(FATAL) << "[txn]BatchGet NewReader failed, start_ts: " << start_ts;
  }

  // for every key in keys, get lock info, if lock_ts < start_ts, return LockInfo
  // else find the latest write below our start_ts
  // then read data from data_cf
  for (const auto &key : keys) {
    pb::common::KeyValue kv;
    kv.set_key(key);

    pb::store::LockInfo lock_info;
    auto ret = GetLockInfo(lock_reader, key, lock_info);
    if (!ret.ok()) {
      DINGO_LOG(FATAL) << "[txn]BatchGet GetLockInfo failed, key: " << key << ", status: " << ret.error_str();
    }

    // if lock_info is exists, check if lock_ts < start_ts
    if (lock_info.lock_ts() > 0 && lock_info.lock_ts() < start_ts) {
      // lock_ts < start_ts, return lock_info
      *(txn_result_info.mutable_locked()) = lock_info;
      return butil::Status::OK();
    }

    IteratorOptions iter_options;
    iter_options.lower_bound = Helper::EncodeTxnKey(key, start_ts);
    iter_options.upper_bound = Helper::EncodeTxnKey(key, 0);
    auto iter = engine->NewIterator(Constant::kTxnWriteCF, iter_options);
    if (iter == nullptr) {
      DINGO_LOG(FATAL) << "[txn]BatchGet NewIterator failed, start_ts: " << start_ts;
    }

    // if the key is committed after start_ts, return WriteConflict
    iter->SeekToFirst();
    while (iter->Valid()) {
      if (iter->Key().length() <= 8) {
        DINGO_LOG(FATAL) << ", invalid write_key, key: " << iter->Key() << ", start_ts: " << start_ts
                         << ", write_key is less than 8 bytes: " << iter->Key();
      }
      std::string write_key;
      uint64_t write_ts;
      Helper::DecodeTxnKey(iter->Key(), write_key, write_ts);

      if (write_ts < start_ts) {
        // write_ts < start_ts, return write_info
        pb::store::WriteInfo write_info;
        auto ret = write_info.ParseFromArray(iter->Value().data(), iter->Value().size());
        if (!ret) {
          DINGO_LOG(FATAL) << "[txn]BatchGet parse write info failed, key: " << key << ", write_key: " << iter->Key()
                           << ", write_value(hex): " << Helper::StringToHex(iter->Value());
        }

        if (!write_info.short_value().empty()) {
          kv.set_value(write_info.short_value());
          break;
        }

        auto ret1 = data_reader->KvGet(Helper::EncodeTxnKey(key, write_info.start_ts()), *kv.mutable_value());
        if (!ret1.ok() && ret1.error_code() != pb::error::Errno::EKEY_NOT_FOUND) {
          DINGO_LOG(FATAL) << "[txn]BatchGet read data failed, key: " << key << ", status: " << ret1.error_str();
        } else if (ret1.error_code() == pb::error::Errno::EKEY_NOT_FOUND) {
          DINGO_LOG(ERROR) << "[txn]BatchGet read data failed, data is illegally not found, key: " << key
                           << ", status: " << ret1.error_str()
                           << ", raw_key: " << Helper::EncodeTxnKey(key, write_info.start_ts());
        }
        break;
      } else {
        DINGO_LOG(ERROR) << "[txn]BatchGet write_ts: " << write_ts << " >= start_ts: " << start_ts << ", key: " << key
                         << ", write_key: " << iter->Key();
      }

      iter->Next();
    }

    kvs.emplace_back(kv);
  }

  return butil::Status::OK();
}

butil::Status TxnEngineHelper::Scan(const std::shared_ptr<RawEngine> &engine,
                                    const pb::store::IsolationLevel &isolation_level, uint64_t start_ts,
                                    const pb::common::Range &range, uint64_t limit, bool key_only, bool is_reverse,
                                    bool disable_coprocessor, const pb::store::Coprocessor &coprocessor,
                                    pb::store::TxnResultInfo &txn_result_info, std::vector<pb::common::KeyValue> &kvs,
                                    bool &has_more, std::string &end_key) {
  DINGO_LOG(INFO) << "[txn]Scan start_ts: " << start_ts << ", range: " << range.ShortDebugString()
                  << ", isolation_level: " << isolation_level << ", start_ts: " << start_ts << ", limit: " << limit
                  << ", key_only: " << key_only << ", is_reverse: " << is_reverse
                  << ", disable_coprocessor: " << disable_coprocessor << ", coprocessor: " << coprocessor.DebugString()
                  << ", txn_result_info: " << txn_result_info.ShortDebugString();

  if (engine == nullptr) {
    DINGO_LOG(FATAL) << "[txn]Scan engine is null";
  }

  if (limit == 0) {
    return butil::Status::OK();
  }

  if (!kvs.empty()) {
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "kvs is not empty");
  }

  if (has_more || !end_key.empty()) {
    return butil::Status(pb::error::Errno::EILLEGAL_PARAMTETERS, "has_more or end_key is not empty");
  }

  auto snapshot = engine->NewSnapshot();
  if (snapshot == nullptr) {
    DINGO_LOG(FATAL) << "[txn]Scan NewSnapshot failed, start_ts: " << start_ts;
  }

  auto data_reader = engine->NewReader(Constant::kTxnDataCF);
  if (data_reader == nullptr) {
    DINGO_LOG(FATAL) << "[txn]Scan NewReader failed, start_ts: " << start_ts;
  }

  // construct lock iter
  IteratorOptions write_iter_options;
  write_iter_options.lower_bound = Helper::EncodeTxnKey(range.start_key(), start_ts);
  write_iter_options.upper_bound = Helper::EncodeTxnKey(range.end_key(), 0);

  auto write_iter = engine->NewIterator(Constant::kTxnWriteCF, snapshot, write_iter_options);
  if (write_iter == nullptr) {
    DINGO_LOG(FATAL) << "[txn]Scan NewIterator write failed, start_ts: " << start_ts;
  }

  // construct lock iter
  IteratorOptions lock_iter_options;
  lock_iter_options.lower_bound = range.start_key();
  lock_iter_options.upper_bound = range.end_key();

  auto lock_iter = engine->NewIterator(Constant::kTxnLockCF, snapshot, lock_iter_options);
  if (lock_iter == nullptr) {
    DINGO_LOG(FATAL) << "[txn]Scan NewIterator lock failed, start_ts: " << start_ts;
  }

  // iter write and lock iter, if lock_ts < start_ts, return LockInfo

  return butil::Status::OK();
}

butil::Status TxnEngineHelper::GetWriteInfo(const std::shared_ptr<RawEngine> &engine, uint64_t min_commit_ts,
                                            uint64_t max_commit_ts, uint64_t start_ts, const std::string &key,
                                            bool include_rollback, bool include_delete, bool include_put,
                                            pb::store::WriteInfo &write_info, uint64_t &commit_ts) {
  IteratorOptions iter_options;
  iter_options.lower_bound = Helper::EncodeTxnKey(key, max_commit_ts);
  iter_options.upper_bound = Helper::EncodeTxnKey(key, min_commit_ts);
  auto iter = engine->NewIterator(Constant::kTxnWriteCF, iter_options);
  if (iter == nullptr) {
    DINGO_LOG(FATAL) << ", new iterator failed, key: " << Helper::StringToHex(key);
  }

  // if the key is committed after start_ts, return WriteConflict
  pb::store::WriteInfo tmp_write_info;
  iter->SeekToFirst();
  while (iter->Valid()) {
    if (iter->Key().length() <= 8) {
      DINGO_LOG(ERROR) << "invalid write_key, key: " << Helper::StringToHex(iter->Key())
                       << ", write_key is less than 8 bytes: " << Helper::StringToHex(iter->Key());
      return butil::Status(pb::error::Errno::EINTERNAL, "invalid write_key");
    }

    std::string write_key;
    uint64_t write_ts;
    Helper::DecodeTxnKey(iter->Key(), write_key, write_ts);

    if (write_ts < min_commit_ts) {
      break;
    }

    auto ret = tmp_write_info.ParseFromArray(iter->Value().data(), iter->Value().size());
    if (!ret) {
      DINGO_LOG(ERROR) << "cannot parse tmp_write_info, key: " << Helper::StringToHex(iter->Key())
                       << ", write_ts: " << write_ts << ", write_key: " << Helper::StringToHex(iter->Key())
                       << ", write_value(hex): " << Helper::StringToHex(iter->Value());
      return butil::Status(pb::error::Errno::EINTERNAL, "cannot parse tmp_write_info");
    }

    if (start_ts > 0) {
      if (tmp_write_info.start_ts() != start_ts) {
        iter->Next();
        continue;
      }
    }

    if (tmp_write_info.op() == pb::store::Op::Rollback) {
      if (!include_rollback) {
        iter->Next();
        continue;
      }
    } else if (tmp_write_info.op() == pb::store::Op::Delete) {
      if (!include_delete) {
        iter->Next();
        continue;
      }
    } else if (tmp_write_info.op() == pb::store::Op::Put) {
      if (!include_put) {
        iter->Next();
        continue;
      }
    } else {
      DINGO_LOG(ERROR) << "invalid write op, key: " << Helper::StringToHex(iter->Key()) << ", write_ts: " << write_ts
                       << ", write_key: " << Helper::StringToHex(iter->Key())
                       << ", write_value(hex): " << Helper::StringToHex(iter->Value());
      iter->Next();
      continue;
    }

    write_info = tmp_write_info;
    commit_ts = write_ts;
    break;
  }

  return butil::Status::OK();
}

butil::Status TxnEngineHelper::GetRollbackInfo(const std::shared_ptr<RawEngine::Reader> &write_reader,
                                               uint64_t start_ts, const std::string &key,
                                               pb::store::WriteInfo &write_info) {
  std::string write_value;
  auto ret = write_reader->KvGet(Helper::EncodeTxnKey(key, start_ts), write_value);
  if (ret.error_code() == pb::error::Errno::EKEY_NOT_FOUND) {
    // no rollback
    DINGO_LOG(INFO) << "not find a rollback write line, key: " << Helper::StringToHex(key)
                    << ", start_ts: " << start_ts;

    return butil::Status::OK();
  }

  if (write_value.empty()) {
    DINGO_LOG(ERROR) << "find a rollback write line, but write_value is empty, key: " << Helper::StringToHex(key)
                     << ", start_ts: " << start_ts;
    return butil::Status(pb::error::Errno::EINTERNAL, "write_value is empty");
  }

  pb::store::WriteInfo tmp_write_info;
  auto ret1 = tmp_write_info.ParseFromArray(write_value.data(), write_value.size());
  if (!ret1) {
    DINGO_LOG(ERROR) << "parse write info failed, key: " << key << ", start_ts: " << start_ts
                     << ", tmp_write_info: " << tmp_write_info.ShortDebugString()
                     << ", write_value: " << Helper::StringToHex(write_value);
    return butil::Status(pb::error::Errno::EINTERNAL, "parse write info failed");
  }

  if (tmp_write_info.op() == pb::store::Op::Rollback) {
    if (tmp_write_info.start_ts() == start_ts) {
      write_info = tmp_write_info;
      return butil::Status::OK();
    } else {
      DINGO_LOG(ERROR) << "find a rollback write line, but not the same start_ts, there my be FATAL error, write_info: "
                       << tmp_write_info.ShortDebugString() << ", key: " << Helper::StringToHex(key)
                       << ", start_ts: " << start_ts;
    }
  } else {
    DINGO_LOG(ERROR) << "find a rollback write line, but not rollback op, there may be FATAL error, write_info: "
                     << tmp_write_info.ShortDebugString() << ", key: " << Helper::StringToHex(key)
                     << ", start_ts: " << start_ts;
    return butil::Status(pb::error::Errno::EINTERNAL, "find a rollback write line, but not rollback op");
  }

  return butil::Status::OK();
}

}  // namespace dingodb