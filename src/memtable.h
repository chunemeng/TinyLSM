#ifndef MEMTABLE_H
#define MEMTABLE_H
#include <string>
#include <list>
#include "../utils/arena.h"
#include "skiplist.h"
#include "../utils/iterator.h"
namespace lsm {
	using key_type = uint64_t;
	using value_type = std::string;

	class MemTable {
	public:
		typedef lsm::Skiplist<Slice, Slice> Table;
		void put(key_type key, value_type&& val);
		void put(key_type key, const value_type& val);
		std::string get(key_type key) const;
		bool del(key_type key);
		explicit MemTable();
		Iterator* newIterator();
		size_t memoryUsage() const;
	private:
		friend class MemTableIterator;
		uint32_t size;
		Arena arena;
		Table table;
		const char* tombstone;
	};
}

#endif //MEMTABLE_H
