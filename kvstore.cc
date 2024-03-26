#include "kvstore.h"
#include "src/builder.h"
#include <string>

KVStore::KVStore(const std::string& dir, const std::string& vlog)
	: KVStoreAPI(dir, vlog), dbname(dir), vlog_path(vlog),
	  mem(new lsm::MemTable()), v(new lsm::Version()) {
}

KVStore::~KVStore() {
	delete mem;
	delete v;
}

/**
 * Insert/Update the key-value pair.
 * No return values for simplicity.
 */
void KVStore::put(uint64_t key, const std::string& s) {
	if (mem->memoryUsage() == MEM_MAX_SIZE) {
		writeLevel0Table(mem);
	}
	mem->put(key, s);
}
/**
 * Returns the (string) value of the given key.
 * An empty string indicates not found.
 */
std::string KVStore::get(uint64_t key) {
	return mem->get(key);
}
/**
 * Delete the given key-value pair if it exists.
 * Returns false iff the key is not found.
 */
bool KVStore::del(uint64_t key) {
	return mem->del(key);
}

/**
 * This resets the kvstore. All key-value pairs should be removed,
 * including memtable and all sstables files.
 */
void KVStore::reset() {
}

/**
 * Return a list including all the key-value pair between key1 and key2.
 * keys in the list should be in an ascending order.
 * An empty string indicates not found.
 */
void KVStore::scan(uint64_t key1, uint64_t key2, std::list<std::pair<uint64_t, std::string>>& list) {
}

/**
 * This reclaims space from vLog by moving valid value and discarding invalid value.
 * chunk_size is the _size in byte you should AT LEAST recycle.
 */
void KVStore::gc(uint64_t chunk_size) {
}
int KVStore::writeLevel0Table(lsm::MemTable* memtable) {
	lsm::Iterator* iter = memtable->newIterator();
	BuildTable(dbname, v, iter);
	return 0;
}
