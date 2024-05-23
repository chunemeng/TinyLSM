// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "testutil.h"

#include <string>

namespace LSMKV {
    namespace testutils {

        Slice RandomString(std::mt19937 *rnd, int len, std::string *dst) {
            dst->resize(len);
            auto mtx = *rnd;
            std::uniform_int_distribution<int> dist(0, 95);
            for (int i = 0; i < len; i++) {
                (*dst)[i] = static_cast<char>(' ' + (uint32_t) dist(mtx));  // ' ' .. '~'
            }
            return Slice(*dst);
        }

        std::string RandomKey(std::mt19937 *rnd, int len) {
            // Make sure to generate a wide variety of characters so we
            // test the boundary conditions for short-key optimizations.
            static const char kTestChars[] = {'\0', '\1', 'a', 'b', 'c',
                                              'd', 'e', '\xfd', '\xfe', '\xff'};
            std::string result;
            auto mtx = *rnd;
            std::uniform_int_distribution<int> dist(0, sizeof(kTestChars));
            for (int i = 0; i < len; i++) {
                result += kTestChars[(uint32_t) dist(mtx)];
            }
            return result;
        }


    }  // namespace test
}  // namespace leveldb
