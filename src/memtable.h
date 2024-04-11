#ifndef MEMTABLE_H
#define MEMTABLE_H
#include <string>
#include <list>
#include "../utils/arena.h"
#include "skiplist.h"
#include "../utils/iterator.h"
#include <functional>
namespace LSMKV {
	using key_type = uint64_t;
	using value_type = std::string;

	class MemTable {
	public:
		typedef Skiplist<uint64_t, Slice> Table;

		explicit MemTable();
		void put(key_type key, value_type&& val);
		void put(key_type key, const value_type& val);
		void put(key_type key, const Slice& val);
		void put(key_type key, Slice&& val);
		std::string get(key_type key) const;
		bool del(key_type key);
        bool contains(uint64_t);
		bool DELETED(const std::string &s);
        bool DELETED(const Slice &s);
        char* reserve(size_t key);
		Iterator* newIterator();
		size_t memoryUsage() const;
		~MemTable();
	private:
		friend class MemTableIterator;
		uint32_t size = 0;
        Arena arena;
        Table table;
		static constexpr const char* tombstone = "~DELETED~";

    };
}

#endif //MEMTABLE_H
