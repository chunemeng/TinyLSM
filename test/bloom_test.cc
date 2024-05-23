// Copyright (c) 2012 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "gtest/gtest.h"
#include "../utils/coding.h"
#include "testutil.h"
#include "../utils//bloomfilter.h"

namespace LSMKV {

    static const int kVerbose = 1;

    static Slice Key(int i, char *buffer) {
        EncodeFixed32(buffer, i);
        return Slice(buffer, sizeof(uint32_t));
    }

    class BloomTest : public testing::Test {
    public:
        BloomTest() {}

        ~BloomTest() {}

        void Reset() {
            keys_.clear();
            filter_.clear();
        }

        void Add(const Slice &s) { keys_.push_back(s.toString()); }

        void Build() {
            std::vector<Slice> key_slices;
            for (size_t i = 0; i < keys_.size(); i++) {
                key_slices.push_back(Slice(keys_[i]));
            }
            filter_.clear();
            CreateFilter(&key_slices[0], static_cast<int>(key_slices.size()), 1, bloom_sz, &filter_);
            keys_.clear();
        }

        size_t FilterSize() const { return filter_.size(); }

        void DumpFilter() {
            std::fprintf(stderr, "F(");
            for (size_t i = 0; i + 1 < filter_.size(); i++) {
                const unsigned int c = static_cast<unsigned int>(filter_[i]);
                for (int j = 0; j < 8; j++) {
                    std::fprintf(stderr, "%c", (c & (1 << j)) ? '1' : '.');
                }
            }
            std::fprintf(stderr, ")\n");
        }


        bool Matches(const Slice &s) {
            if (!keys_.empty()) {
                Build();
            }
            return KeyMayMatch(s, filter_);
        }

        double FalsePositiveRate() {
            char buffer[sizeof(int)];
            int result = 0;
            for (int i = 0; i < 10000; i++) {
                if (Matches(Key(i + 1000000000, buffer))) {
                    result++;
                }
            }
            return result / 10000.0;
        }

    private:
        std::string filter_;
        std::vector<std::string> keys_;
    protected:
        int bloom_sz = 8192;
    };

    TEST_F(BloomTest, EmptyFilter) {
        ASSERT_TRUE(!Matches("hello"));
        ASSERT_TRUE(!Matches("world"));
    }

    TEST_F(BloomTest, Small) {
        Add("hello");
        Add("world");
        ASSERT_TRUE(Matches("hello"));
        ASSERT_TRUE(Matches("world"));
        ASSERT_TRUE(!Matches("x"));
        ASSERT_TRUE(!Matches("foo"));
    }

    static int NextLength(int length) {
        length += 100;
        return length;
    }

    TEST_F(BloomTest, VaryingLengths) {
        char buffer[sizeof(int)];

        // Count number of filters that significantly exceed the false positive rate
        int mediocre_filters = 0;
        int good_filters = 0;

        for (int length = 100; length <= 800; length = NextLength(length)) {
            Reset();
            for (int i = 0; i < length; i++) {
                Add(Key(i, buffer));
            }
            bloom_sz = 16 * 1024 - 32 - length * 20;
            Build();


            // All added keys must match
            for (int i = 0; i < length; i++) {
                ASSERT_TRUE(Matches(Key(i, buffer)))
                                            << "Length " << length << "; key " << i;
            }

            // Check false positive rate
            double rate = FalsePositiveRate();

            std::fprintf(stderr,
                         "False positives: %5.2f%% @ length = %6d ; bytes = %6d\n",
                         rate * 100.0, length, static_cast<int>(FilterSize()));

//            ASSERT_LE(rate, 0.025);  // Must not be over 2%
            if (rate > 0.0125)
                mediocre_filters++;  // Allowed, but not too often
            else
                good_filters++;
        }
    }

// Different bits-per-byte

}  // namespace leveldb
