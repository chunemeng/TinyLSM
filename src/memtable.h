#ifndef MEMTABLE_H
#define MEMTABLE_H
#include <string>
#include <list>
#include "../utils/arena.h"
#include "skiplist.h"
#include "../utils/iterator.h"
namespace LSMKV {
	using key_type = uint64_t;
	using value_type = std::string;

	class MemTable {
	public:
		typedef Skiplist<uint64_t, Slice> Table;
		void put(key_type key, value_type&& val);
		void put(key_type key, const value_type& val);
		void put(key_type key, const Slice& val);
		void put(key_type key, Slice&& val);
		std::string get(key_type key) const;
		bool del(key_type key);
		char* reserve(size_t key);
		const char* DeleteFlag() const;
		explicit MemTable();
		Iterator* newIterator();
		size_t memoryUsage() const;
		~MemTable();
	private:
		friend class MemTableIterator;
		uint32_t size;
		Arena arena;
		Table table;
		static constexpr const char* tombstone = "~DELETED~\0";
	};
}

#endif //MEMTABLE_H
