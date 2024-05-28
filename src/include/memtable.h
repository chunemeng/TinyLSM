#ifndef MEMTABLE_H
#define MEMTABLE_H
#include <string>
#include <list>
#include "../../utils/arena.h"
#include "skiplist.h"
#include "../../utils/iterator.h"
#include <functional>
#include <map>

namespace LSMKV {
	using key_type = uint64_t;
	using value_type = std::string;
//#define rbtree
	class MemTable {
	public:
#ifdef rbtree
        typedef std::map<uint64_t, Slice> Table;
#else
		typedef Skiplist<uint64_t, Slice> Table;
#endif
		explicit MemTable();
		void put(key_type key, value_type&& val);
		void put(key_type key, const value_type& val);
		void put(key_type key, const Slice& val);
		void put(key_type key, Slice&& val);
		[[nodiscard]] std::string get(key_type key) const;
		bool del(key_type key);
        bool contains(uint64_t);
		static bool DELETED(const std::string &s);
        static bool DELETED(const Slice &s);
        char* reserve(size_t key);
		Iterator* newIterator();
		[[nodiscard]] size_t memoryUsage() const;
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
