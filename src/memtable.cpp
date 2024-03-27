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
				if (iter.value() == Slice(tombstone, 10)) {
					return {};
				}
				return iter.value().toString();
			}
		}
		return "";
	}
	MemTable::MemTable() : table(&arena), size(0) {
		tombstone = arena.allocate(10);
		tombstone = "~DELETED~\0";
	}

	bool MemTable::del(key_type key) {
		return table.remove(key, Slice(tombstone, 10));
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
}
