#include <sstream>
#include "memtable.h"
#include "../utils/coding.h"

//template<class T>
//static Slice SliceHelper(const T& t) {
//	std::stringstream tmp;
//	tmp << t;
//	return { tmp.str().c_str(), sizeof(T) };
//}

namespace LSMKV {
	class MemTableIterator : public Iterator {
	public:
		explicit MemTableIterator(MemTable::Table* table) : _iter(table) {
		}

		MemTableIterator(const MemTableIterator&) = delete;
		MemTableIterator& operator=(const MemTableIterator&) = delete;

		~MemTableIterator() override = default;

		bool hasNext() const override {
			return _iter.hasNext();
		}
		void seek(const key_type& k) override {
			_iter.seek(k);
		}
		void seekToFirst() override {
			_iter.seekToFirst();
		}
		void next() override {
			_iter.next();
		}
		void scan(const uint64_t& K1,
			const uint64_t& K2,
			std::list<std::pair<uint64_t, std::string>>& list) override {
			_iter.seek(K1, K2);
			for (; _iter.hasNext(); _iter.next()) {
				list.emplace_back(key(), _iter.value().toString());
			}
		}
		uint64_t key() const override {
			return _iter.key();
		}
		Slice value() const override {
			return _iter.value() ;
		}

	private:
		MemTable::Table::Iterator _iter;
	};

	Iterator* MemTable::newIterator() {
		return new MemTableIterator(&table);
	}

	void MemTable::put(key_type key, value_type&& val) {
		char* buf = arena.allocate(val.size());
		memcpy(buf, val.data(), val.size());
		size += table.insert(key, Slice(buf, val.size()));
	}
	value_type MemTable::get(key_type key) const {
		Table::Iterator iter(&table);
		iter.seek(key);
		if (iter.hasNext()) {
			if (iter.key() == key) {
				return iter.value().toString();
			}
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
	void MemTable::put(key_type key, const value_type& val) {
		char* buf = arena.allocate(val.size());
		memcpy(buf, val.data(), val.size());
		size += table.insert(key, Slice(buf, val.size()));
	}

	MemTable::~MemTable() {
	}

	void MemTable::put(key_type key, const Slice& val) {
		char* buf = arena.allocate(val.size());
		memcpy(buf, val.data(), val.size());
		size += table.insert(key, Slice(buf, val.size()));
	}
	void MemTable::put(key_type key, Slice&& val) {
		size += table.insert(key, std::move(val));
	}
	char* MemTable::reserve(size_t key_size) {
		return arena.allocate(key_size);
	}
	bool MemTable::DELETED(const std::string& s) {
		return s == tombstone;
	}
}
