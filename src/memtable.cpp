#include "memtable.h"
#include "utils/coding.h"
#include <iostream>
#include <utility>

namespace LSMKV {
#ifdef rbtree
class MapIter {
public:
    MapIter(MemTable::Table *table) : table_(table) {
        _cur = table->end();
        _end = table->end();
    }

    [[nodiscard]] bool hasNext() const {
        return _end != _cur && _cur != table_->end();
    }

    const key_type &key() const {
        return _cur->first;
    }

    void next() {
        _cur++;
    }

    void seek(const key_type &key) {
        _cur = table_->find(key);
    }

    void seek(const key_type &K1, const key_type &K2) {
        _cur = table_->lower_bound(K1);
        _end = table_->upper_bound(K2);
        //            if (_end != nullptr && _end->_key == K2) _end = _end->next(0);
    }

    void seekToFirst() {
        _cur = table_->begin();
        _end = table_->end();
    }

    const Slice &value() const {
        return _cur->second;
    }

private:
    const MemTable::Table *table_;
    decltype(table_->begin()) _cur;
    decltype(table_->begin()) _end;
};
#endif

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
#ifdef rbtree
    MapIter _iter;
#else
    MemTable::Table::Iterator _iter;
#endif
};

Iterator *MemTable::newIterator() {
    return new MemTableIterator(&table);
}


#ifdef rbtree
void MemTable::put(key_type key, value_type &&val) {
    char *buf = arena.allocate(val.size());
    memcpy_tiny(buf, val.data(), val.size());
    auto pair = table.insert({key, Slice(buf, val.size())});
    if (!pair.second) {
        pair.first->second = Slice(buf, val.size());
    }
    size += pair.second;
}

value_type MemTable::get(key_type key) const {
    auto it = table.find(key);
    if (it == table.end()) {
        return "";
    } else {
        auto value = it->second;
        auto size = value.size();
        std::string res;
        res.reserve(size);
        res.resize(size);
        memcpy_tiny(res.data(), value.data(), size);
        return std::move(res);
    }
}

MemTable::MemTable() : size(0) {
}

bool MemTable::del(key_type key) {
    // Maybe useless
    auto it = table.find(key);
    if (it == table.end()) {
        return false;
    } else {
        it->second = Slice(tombstone, 9);
        return true;
    }
}

void MemTable::put(key_type key, const value_type &val) {
    char *buf = arena.allocate(val.size());
    memcpy_tiny(buf, val.data(), val.size());
    auto pair = table.insert({key, Slice(buf, val.size())});
    if (!pair.second) {
        pair.first->second = Slice(buf, val.size());
    }
    size += pair.second;
}

// NEED TO STORE VALUE
void MemTable::put(key_type key, const Slice &val) {
    char *buf = arena.allocate(val.size());
    memcpy_tiny(buf, val.data(), val.size());
    auto pair = table.insert({key, Slice(buf, val.size())});
    if (!pair.second) {
        pair.first->second = Slice(buf, val.size());
    }
    size += pair.second;
}

// todo not use
void MemTable::put(key_type key, Slice &&val) {
    size += table.insert({key, std::move(val)}).second;
}
#else

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

void MemTable::put(key_type key, const value_type &val) {
    char *buf = arena.allocate(val.size());
    memcpy_tiny(buf, val.data(), val.size());
    size += table.insert(key, Slice(buf, val.size()));
}


// NEED TO STORE VALUE
void MemTable::put(key_type key, const Slice &val) {
    char *buf = arena.allocate(val.size());
    memcpy_tiny(buf, val.data(), val.size());

    size += table.insert(key, Slice(buf, val.size()));
}


// NO NEED TO STORE VALUE
void MemTable::put(key_type key, Slice &&val) {
    size += table.insert(key, std::move(val));
}

#endif
MemTable::~MemTable(){
        //        std::cout<<"Arena: "<<arena.getWaste()<<std::endl;
};

size_t MemTable::memoryUsage() const {
    return size;
}


char *MemTable::reserve(size_t key_size) {
    return arena.allocate(key_size);
}

bool MemTable::DELETED(const std::string &s) {
    if (s.size() != 9) [[likely]] {
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

}// namespace LSMKV
