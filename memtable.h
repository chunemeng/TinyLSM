#ifndef MEMTABLE_H
#define MEMTABLE_H
#include <string>
#include <list>
#include "utils/arena.h"
#include "skiplist.h"
#include "utils/iterator.h"

using key_type = uint64_t;
using value_type = std::string;
#define MAX_SIZE 408;

class MemTable {
public:
	typedef Skiplist::Skiplist<key_type , value_type> Table;
	void put(key_type key, value_type&& val);
	std::string get(key_type key) const;
	bool del(key_type key);
	explicit MemTable();
	Iterator* newIterator();
	bool memoryUsage() const;
private:
	friend class MemTableIterator;
	uint32_t size;
	Arena arena;
	Table table;
};

#endif //MEMTABLE_H
