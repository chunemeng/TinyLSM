// Copyright (c) 2012 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "../utils//bloomfilter.h"
#include "../utils/coding.h"
#include "testutil.h"
#include "gtest/gtest.h"

namespace LSMKV {

static const int kVerbose = 1;

static Slice Key(int i, char *buffer) {
  EncodeFixed32(buffer, i);
  return Slice(buffer, sizeof(uint32_t));
}

class SameTimeStampTest : public testing::Test {
 public:
  SameTimeStampTest() : store("./data", "./data/vlog") { store.reset(); }

  ~SameTimeStampTest() override {}

  void Build() {
    std::string key{"abcdefg"};
    std::string not_equal{"gfedcba"};
    const int step = 408;
    int start{};
    store.reset();

    for (int j = 1; j < 7; j++) {
      start = 1000 * j;
      for (int i = start; i < start + step; i++) {
        store.put(i, not_equal);
      }
    }

    start = 1600;
    for (int i = start; i < start + step; ++i) {
      store.put(i, not_equal);
    }
    start = 2200;
    for (int i = start; i < start + step; ++i) {
      store.put(i, not_equal);
    }

    start = 3000;
    for (int i = start; i < start + step; ++i) {
      store.put(i, not_equal);
    }

    for (int i = 1980; i < 1500 + 408 * 20; i += 20) {
      store.put(i, key);
    }

    for (int i = 1980; i < 1500 + 408 * 20; i += 20) {
      store.put(i, key);
    }

    for (int i = 1980; i < 1500 + 408 * 20; i += 20) {
      store.put(i, key);
    }

    start = 10000;
    for (int j = 0; j < 3; ++j)
      for (int i = start; i < start + step; i++) {
        store.put(i, not_equal);
      }

  }

 public:
  KVStore store;
};

TEST_F(SameTimeStampTest, SameTimeStamp) {
  std::string not_equal{"gfedcba"};
  for (int i = 0; i < 208; ++i) {
    if (i % 20 == 0) {
      continue;
    } else {
      EXPECT_EQ(store.get(i), not_equal);
    }
  }

}

// Different bits-per-byte

} // namespace LSMKV
