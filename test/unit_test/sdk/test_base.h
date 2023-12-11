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

#ifndef DINGODB_SDK_TEST_TEST_BASE_H_
#define DINGODB_SDK_TEST_TEST_BASE_H_

#include <memory>

#include "client.h"
#include "gmock/gmock.h"
#include "gtest/gtest.h"
#include "meta_cache.h"
#include "mock_client_stub.h"
#include "mock_coordinator_proxy.h"
#include "mock_region_scanner.h"
#include "mock_rpc_interaction.h"
#include "raw_kv_impl.h"
#include "test_common.h"

namespace dingodb {
namespace sdk {
class TestBase : public ::testing::Test {
 public:
  TestBase() {
    coordinator_proxy = std::make_shared<MockCoordinatorProxy>();
    meta_cache = std::make_shared<MetaCache>(coordinator_proxy);

    brpc::ChannelOptions options;
    options.connect_timeout_ms = 3000;
    options.timeout_ms = 5000;
    store_rpc_interaction = std::make_shared<MockRpcInteraction>(options);

    region_scanner_factory = std::make_shared<MockRegionScannerFactory>();

    ON_CALL(*coordinator_proxy, ScanRegions).WillByDefault(testing::Return(Status::OK()));

    stub = std::make_unique<MockClientStub>();

    ON_CALL(*stub, GetCoordinatorProxy).WillByDefault(testing::Return(coordinator_proxy));
    EXPECT_CALL(*stub, GetCoordinatorProxy).Times(testing::AnyNumber());

    ON_CALL(*stub, GetMetaCache).WillByDefault(testing::Return(meta_cache));
    EXPECT_CALL(*stub, GetMetaCache).Times(testing::AnyNumber());

    ON_CALL(*stub, GetStoreRpcInteraction).WillByDefault(testing::Return(store_rpc_interaction));
    EXPECT_CALL(*stub, GetStoreRpcInteraction).Times(testing::AnyNumber());

    ON_CALL(*stub, GetRegionScannerFactory).WillByDefault(testing::Return(region_scanner_factory));
    EXPECT_CALL(*stub, GetRegionScannerFactory).Times(testing::AnyNumber());
  }

  ~TestBase() override {
    stub.reset();
    store_rpc_interaction.reset();
    meta_cache.reset();
  }

  void SetUp() override { PreFillMetaCache(); }

  Status NewRawKV(std::shared_ptr<RawKV>& raw_kv) const {
    std::shared_ptr<RawKV> ret(new RawKV(new RawKV::RawKVImpl(*stub)));
    raw_kv = ret;
    return Status::OK();
  }

  std::shared_ptr<MockCoordinatorProxy> coordinator_proxy;
  std::shared_ptr<MetaCache> meta_cache;
  std::shared_ptr<MockRpcInteraction> store_rpc_interaction;
  std::shared_ptr<MockRegionScannerFactory> region_scanner_factory;

  std::unique_ptr<MockClientStub> stub;

 private:
  void PreFillMetaCache() {
    meta_cache->MaybeAddRegion(RegionA2C());
    meta_cache->MaybeAddRegion(RegionC2E());
    meta_cache->MaybeAddRegion(RegionE2G());
  }
};

}  // namespace sdk
}  // namespace dingodb
#endif  // DINGODB_SDK_TEST_TEST_BASE_H_