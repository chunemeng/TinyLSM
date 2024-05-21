#ifndef LSMKV_SRC_TABLE_H
#define LSMKV_SRC_TABLE_H

#include <string>
#include "memtable.h"
#include "../../utils/option.h"
#include "../../utils/coding.h"
#include "../../utils/bloomfilter.h"
#include <map>
#include <unordered_set>
#include <iostream>

namespace LSMKV {
    //instance of sst
    class Table {
    public:
        explicit Table(const uint64_t &file_no) : file_no(file_no) {
        }

        char *reserve(size_t size) {
            return arena.allocate(size);
        }

        explicit Table(const char *tmp, const uint64_t &file_no) {
            timestamp = DecodeFixed64(tmp);
            this->file_no = file_no;
//            if constexpr (!LSMKV::Option::isIndex) {
//                return;
//            }

            uint64_t size = DecodeFixed64(tmp + 8);
            uint64_t offset = bloom_size;
            sst.reserve(size);
            char *buf;
            if ((isFilter = LSMKV::Option::isFilter)) {
                buf = arena.allocate(size * 20 + bloom_size);
            } else {
                buf = arena.allocate(size * 20);
            }
            bloom = Slice(buf, 8192);

            // NO NEED TO COPY HEADER
            // IF OPEN FILTER COPY IT
            memcpy(buf, tmp + 32 + !isFilter * 8192, (isFilter ? 8192 : 0) + size * 20);

            // TODO MAY NEED COMPARE!!
            for (uint64_t i = 0; i < size * 20; i += 20) {
                sst.emplace_back(DecodeFixed64(buf + offset + i), Slice(buf + offset + 8 + i, 12));
            }

        }

        void pushCache(const char *tmp) {
            if ((isFilter = LSMKV::Option::isFilter)) {
                bloom = Slice(tmp + 32, 8192);
            }
            timestamp = DecodeFixed64(tmp);
            uint64_t size = DecodeFixed64(tmp + 8);
            uint64_t offset = bloom_size + 32;

            // TODO MAY NEED COMPARE!!
            sst.reserve(size);
            for (uint64_t i = 0; i < size * 20; i += 20) {
                sst.emplace_back(DecodeFixed64(tmp + offset + i), Slice(tmp + offset + 8 + i, 12));
            }
        }

        [[nodiscard]] uint64_t GetTimestamp() const {
            return timestamp;
        }

        [[nodiscard]] bool KeyMatch(const uint64_t &key) const {
            if (key < sst[0].first || key > sst[sst.size() - 1].first) {
                return false;
            }
            return !isFilter || KeyMayMatch(key, bloom);
        }

        ~Table() = default;

        bool operator<(const Table &rhs) const {
            return timestamp < rhs.timestamp;
        }

        bool operator<(const uint64_t &rhs) const {
            return timestamp < rhs;
        }

        std::string get(const uint64_t &key) {
            if (!KeyMatch(key)) {
                return "";
            }
            uint64_t offset = BinarySearchGet(key);
            if (offset >= sst.size()) {
                return "";
            } else {
                return {sst[offset].second.data(), sst[offset].second.size()};
            }
        }

    private:
        [[nodiscard]] uint64_t BinarySearchGet(const uint64_t &key) const {
            // TODO 斐波那契分割优化(?)
            // NOT INCLUDE END
            uint64_t left = 0, right = sst.size(), mid;
            while (left < right) {
                mid = left + ((right - left) >> 1);
                if (sst[mid].first < key)
                    left = mid + 1;
                else
                    right = mid;
            }
            // To avoid False positive
            if (left <= sst.size() && sst[left].first == key) {
                return left;
            }

            return sst.size();
//			assert(start < end && start < sst.size() && end < sst.size());
//			uint64_t low = start + 1, high = end - 1, mid;
//			if (key == sst[start].first) {
//				return start;
//			}
//			while (low <= high) {
//				mid = low + ((high - low) >> 1);
//				if (key < sst[mid].first)
//					high = mid - 1;
//				else if (key > sst[mid].first)
//					low = mid + 1;
//				else
//					return mid;
//			}
//			return sst.size();
        }

        [[nodiscard]] uint64_t BinarySearchLocation(const uint64_t &start,
                                                    const uint64_t &end,
                                                    const uint64_t &key) const {
            // TODO 斐波那契分割优化(?)
            // NOT INCLUDE END
            uint64_t left = start, right = end, mid;
            while (left < right) {
                mid = left + ((right - left) >> 1);
                if (sst[mid].first < key)
                    left = mid + 1;
                else
                    right = mid;
            }
            return left;
        }

        bool isFilter = true;
        Arena arena;
        std::vector<std::pair<uint64_t, Slice>> sst;
        Slice bloom;
        uint64_t file_no = 0;
        uint64_t timestamp = 0;
    public:
        class TableIterator {
        public:
            explicit TableIterator(Table *table) : _table(table), _cur(0) {
                _end = _table->sst.size();
            };

            ~TableIterator() {
                delete _table;
                _table = nullptr;
            }

            [[nodiscard]] uint64_t size() const {
                return _table->sst.size();
            }

            [[nodiscard]] bool hasNext() const {
                return _end > _cur;
            }

            [[nodiscard]] Slice value() const {
                return _table->sst[_cur].second;
            }

            [[nodiscard]] uint64_t key() const {
                return _table->sst[_cur].first;
            }

            [[nodiscard]] bool AtStart() const {
                return _cur == 0;
            }

            void next() {
                _cur++;
            }

            void seek(uint64_t key) {
                if (!_table->KeyMatch(key)) {
                    _end = 0;
                    _cur = 1;
                    return;
                }
                _cur = _table->BinarySearchGet(key);
                _end = _table->sst.size();
            }

            void seek(const uint64_t &K1, const uint64_t &K2) {
                if (!InRange(K1, K2)) {
                    _cur = _end + 1;
                }
                _end = _table->sst.size();
                _cur = _table->BinarySearchLocation(0, _end, K1);
                _end = _table->BinarySearchLocation(_cur, _end, K2);
                if (_end < _table->sst.size() && _table->sst[_end].first == K2) {
                    _end++;
                }
            }

            void seekToLast() {
                _end = _table->sst.size();
                _cur = _end - 1;
            }

            [[nodiscard]] uint64_t LargestKey() const {
                return _table->sst[_table->sst.size() - 1].first;
            }

            [[nodiscard]] uint64_t SmallestKey() const {
                return _table->sst[0].first;
            }

            void setFileNo(const uint64_t &file_no) {
                _table->file_no = file_no;
            }

            void setTimestamp(const uint64_t &timestamp) {
                _table->timestamp = timestamp;
            }

            [[nodiscard]] bool InRange(const uint64_t &smallest, const uint64_t &largest) const {
                return std::max(smallest, SmallestKey()) <= std::min(largest, LargestKey());
            }

            void seekToFirst() {
                _cur = 0;
                _end = _table->sst.size();
            }

            [[nodiscard]] uint64_t timestamp() const {
                return _table->timestamp;
            }

            [[nodiscard]] uint64_t file_no() const {
                return _table->file_no;
            }

        private:
            Table *_table;
            // not include _end!!!!
            uint64_t _cur;
            uint64_t _end;
        };

        TableIterator *Iterator() {
            return new Table::TableIterator(this);
        }
    };

}

#endif //LSMKV_SRC_TABLE_H
