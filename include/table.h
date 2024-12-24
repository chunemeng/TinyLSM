#ifndef LSMKV_SRC_TABLE_H
#define LSMKV_SRC_TABLE_H

#include "memtable.h"
#include "utils/bloomfilter.h"
#include "utils/coding.h"
#include "utils/option.h"
#include <iostream>
#include <map>
#include <string>
#include <unordered_set>

namespace LSMKV {
//instance of sst
class Table {
public:
    Table() {
        this->timestamp = 0;
        sst[0] = std::make_pair(UINT64_MAX, Slice());
    }

    explicit Table(const uint64_t &file_no) : file_no(file_no) {
    }
    ~Table() = default;
    char *reserve(size_t size) {
        return arena.allocate(size);
    }

    explicit Table(const char *tmp, const uint64_t &file_no) {
        timestamp = DecodeFixed64(tmp);
        this->file_no = file_no;
        size_ = DecodeFixed64(tmp + 8);

        char *buf;
        if (isFilter = LSMKV::Option::isFilter) {
            buf = arena.allocate(size_ * 20 + bloom_size);
            bloom = Slice(buf, bloom_size);
        } else {
            buf = arena.allocate(size_ * 20);
        }

        // NO NEED TO COPY HEADER
        // IF OPEN FILTER COPY IT
        memcpy(buf, tmp + 32 + !isFilter * bloom_size, (isFilter ? bloom_size : 0) + size_ * 20);
        uint64_t offset = isFilter * bloom_size;

        // TODO MAY NEED COMPARE!!
        for (uint64_t i = 0; i < size_ * 20; i += 20) {
            sst[i / 20] = std::make_pair(DecodeFixed64(buf + offset + i), Slice(buf + offset + 8 + i, 12));
        }
    }

    void pushCache(const char *tmp) {
        if ((isFilter = LSMKV::Option::isFilter)) {
            bloom = Slice(tmp + 32, bloom_size);
        }
        timestamp = DecodeFixed64(tmp);
        size_ = DecodeFixed64(tmp + 8);
        uint64_t offset = bloom_size + 32;

        // TODO MAY NEED COMPARE!!
        //            sst.reserve(size);
        for (uint64_t i = 0; i < size_ * 20; i += 20) {
            sst[i / 20] = std::make_pair(DecodeFixed64(tmp + offset + i), Slice(tmp + offset + 8 + i, 12));
        }
    }

    [[nodiscard]] uint64_t GetTimestamp() const {
        return timestamp;
    }

    [[nodiscard]] bool KeyMatch(const uint64_t &key) const {
        if (key < sst[0].first || key > sst[size() - 1].first) {
            return false;
        }
        return !isFilter || KeyMayMatch(key, bloom);
    }

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
        if (offset >= size()) {
            return "";
        } else {
            return {sst[offset].second.data(), sst[offset].second.size()};
        }
    }

    size_t size() const {
        return size_;
    }

private:
    [[nodiscard]] uint64_t BinarySearchGet(const uint64_t &key) const {
        // TODO 斐波那契分割优化(?)
        // NOT INCLUDE END
        uint64_t left = 0, right = size(), mid;
        while (left < right) {
            mid = left + ((right - left) >> 1);
            if (sst[mid].first < key)
                left = mid + 1;
            else
                right = mid;
        }
        // To avoid False positive
        if (left <= size() && sst[left].first == key) {
            return left;
        }

        return size();
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
    //        std::vector<std::pair<uint64_t, Slice>> sst;
    Slice bloom;
    uint64_t file_no = 0;
    uint64_t timestamp = 0;
    size_t size_;
    std::pair<uint64_t, Slice> sst[0];

public:
    class TableIterator {
    public:
        explicit TableIterator(Table *table) : _table(table){};

        void seekToEmpty() {
            setTimestamp(0);
            _end = 0;
        }

        ~TableIterator() {
            _table->~Table();
            free(_table);
            _table = nullptr;
        }

        [[nodiscard]] uint64_t size() const {
            return _table->size();
        }

        [[nodiscard]] bool hasNext() const {
            return _end > _cur;
        }

        [[nodiscard]] Slice value() const {
            return _table->sst[_cur].second;
        }

        [[nodiscard]] uint64_t merge_key() const {
            bool flag = _cur < _end;
            return !flag * UINT64_MAX + flag * _table->sst[_cur].first;
        }

        void merge_next() {
            if (_cur + 1 == _end) [[unlikely]] {
                seekToEmpty();
            }
            _cur++;
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
            _end = _table->size();
        }

        void seek(const uint64_t &K1, const uint64_t &K2) {
            if (!InRange(K1, K2)) {
                _cur = _end + 1;
            }
            _end = _table->size();
            _cur = _table->BinarySearchLocation(0, _end, K1);
            _end = _table->BinarySearchLocation(_cur, _end, K2);
            if (_end < _table->size() && _table->sst[_end].first == K2) {
                _end++;
            }
        }

        void seekToLast() {
            _end = _table->size();
            _cur = _end - 1;
        }

        [[nodiscard]] uint64_t LargestKey() const {
            return _table->sst[_table->size() - 1].first;
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
            _end = _table->size();
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
        uint64_t _cur{};
        uint64_t _end{};
    };

    TableIterator *Iterator() {
        return new Table::TableIterator(this);
    }
};

}// namespace LSMKV

#endif//LSMKV_SRC_TABLE_H
