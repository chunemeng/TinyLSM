// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef STORAGE_LEVELDB_UTIL_TESTUTIL_H_
#define STORAGE_LEVELDB_UTIL_TESTUTIL_H_

#include "../kvstore.h"
#include "utils/slice.h"
#include "gtest/gtest.h"
#include <random>

namespace LSMKV {
    namespace testutils {


        inline void put(int length, const std::string &s, KVStore *store) {
            for (int i = 0; i < length; i++) {
                store->put(i, s);
            }
        }

        inline int RandomSeed() {
            return testing::UnitTest::GetInstance()->random_seed();
        }


        Slice RandomString(std::mt19937 *rnd, int len, std::string *dst);


        std::string RandomKey(std::mt19937 *rnd, int len);

    }  // namespace test
}  // namespace leveldb

#endif  // STORAGE_LEVELDB_UTIL_TESTUTIL_H_
