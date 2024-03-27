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
			char buf[8];
			EncodeFixed64(buf, k);
			_iter.seek(Slice(buf, 8));
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
			char buf[16];
			EncodeFixed64(buf, K1);
			EncodeFixed64(buf + 8, K2);
			_iter.seek(Slice(buf, 8), Slice(buf + 8, 8));
			for (; _iter.hasNext(); _iter.next()) {
				list.emplace_back(DecodeFixed64(_iter.key().data()), _iter.value().toString());
			}
		}
		Slice key() const override {
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
		char* buf = arena.allocate(val.size() + 8);
		EncodeFixed64(buf, key);
		memcpy(buf + 8, val.data(), val.size());
		size += table.insert(Slice(buf, 8), Slice(buf + 8, val.size()));
	}
	value_type MemTable::get(key_type key) const {
		EncodeFixed64(key_buf, key);
		Table::Iterator iter(&table);
		iter.seek(key_buf);
		if (iter.hasNext()) {
			if (iter.key() == Slice(key_buf,8)) {
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
		EncodeFixed64(key_buf, key);
		return table.remove(Slice(key_buf, 8), Slice(tombstone, 10));
	}
	size_t MemTable::memoryUsage() const {
		return size;
	}
	void MemTable::put(key_type key, const value_type& val) {
		char* buf = arena.allocate(val.size() + 8);
		EncodeFixed64(buf, key);
		memcpy(buf + 8, val.data(), val.size());
		size += table.insert(Slice(buf, 8), Slice(buf + 8, val.size()));
	}
	MemTable::~MemTable() {
		delete
	}
}
