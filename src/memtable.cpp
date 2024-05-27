#include <utility>
#include <iostream>
#include "include/memtable.h"
#include "../utils/coding.h"

namespace LSMKV {
    class MemTableIterator : public Iterator {
    public:
        explicit MemTableIterator(MemTable::Table *table) : _iter(table) {
        }

        MemTableIterator(const MemTableIterator &) = delete;

        MemTableIterator &operator=(const MemTableIterator &) = delete;

        ~MemTableIterator() override = default;

        [[nodiscard]] bool hasNext() const override {
            return _iter.hasNext();
        }

        void seek(const key_type &k) override {
            _iter.seek(k);
        }

        void seekToFirst() override {
            _iter.seekToFirst();
        }

        void next() override {
            _iter.next();
        }

        void scan(const uint64_t &K1,
                  const uint64_t &K2,
                  std::list<std::pair<uint64_t, std::string>> &list) override {
            _iter.seek(K1, K2);
            for (; _iter.hasNext(); _iter.next()) {
                list.emplace_back(key(), _iter.value().toString());
            }
        }

        [[nodiscard]] uint64_t key() const override {
            return _iter.key();
        }

        [[nodiscard]] Slice value() const override {
            return _iter.value();
        }

    private:
        MemTable::Table::Iterator _iter;
    };

    Iterator *MemTable::newIterator() {
        return new MemTableIterator(&table);
    }

    void MemTable::put(key_type key, value_type &&val) {
        char *buf = arena.allocate(val.size());
        memcpy_tiny(buf, val.data(), val.size());
        size += table.insert(key, Slice(buf, val.size()));
    }

    value_type MemTable::get(key_type key) const {
        Table::Iterator iter(&table);
        iter.seek(key);
        if (iter.hasNext() && iter.key() == key) {
            auto value = iter.value();
            auto size = value.size();
            std::string res;
            res.reserve(size);
            res.resize(size);
            memcpy_tiny(res.data(), value.data(), size);
            return std::move(res);
        }
        return "";
    }

    MemTable::MemTable() : table(&arena), size(0) {
    }

    bool MemTable::del(key_type key) {
        // Maybe useless
        return table.remove(key, Slice(tombstone, 9));
    }

    size_t MemTable::memoryUsage() const {
        return size;
    }

    void MemTable::put(key_type key, const value_type &val) {
        char *buf = arena.allocate(val.size());
        memcpy_tiny(buf, val.data(), val.size());
        size += table.insert(key, Slice(buf, val.size()));
    }

    MemTable::~MemTable() {
//        std::cout<<"Arena: "<<arena.getWaste()<<std::endl;
    };

    // NEED TO STORE VALUE
    void MemTable::put(key_type key, const Slice &val) {
        char *buf = arena.allocate(val.size());
        if (val.size() < 64) {
            memcpy_tiny(buf, val.data(), val.size());
        } else {
            memcpy(buf, val.data(), val.size());
        }
        size += table.insert(key, Slice(buf, val.size()));
    }

    // NO NEED TO STORE VALUE
    void MemTable::put(key_type key, Slice &&val) {
        size += table.insert(key, std::move(val));
    }

    char *MemTable::reserve(size_t key_size) {
        return arena.allocate(key_size);
    }

    bool MemTable::DELETED(const std::string &s) {
        if (s.size() != 9) {
            return false;
        } else {
            return !strcmp(tombstone, s.c_str());
        }
    }

    bool MemTable::DELETED(const Slice &s) {
        return s == Slice(tombstone, 9);
    }

    bool MemTable::contains(uint64_t key) {
        return table.contains(key);
    }
}
