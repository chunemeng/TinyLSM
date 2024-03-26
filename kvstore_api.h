#pragma once

#include <cstdint>
#include <string>
#include <list>
#include "utils.h"

class KVStoreAPI {
public:
	/**
	 * You should put all sstables under `dir`.
	 * Please create one sub-directory for each level, and put sstables
	 * there. Please refer to the c++ filesystem library
	 * (https://en.cppreference.com/w/cpp/filesystem).
	 */
	KVStoreAPI(const std::string& dir, const std::string& vlog) {
		if (!utils::dirExists(dir)) {
			utils::_mkdir(dir);
		}
		std::string level_dir = dir + "/" + "level-0";
		size_t size = level_dir.size();
		for (int i = 0; i < 8;++i) {
			level_dir[size] = '0' + i;
			if (!utils::dirExists(level_dir)) {
				utils::_mkdir(level_dir);
			}
		}
	}
	KVStoreAPI() = delete;

	/**
	 * Insert/Update the key-value pair.
	 * No return values for simplicity.
	 */
	virtual void put(uint64_t key, const std::string& s) = 0;

	/**
	 * Returns the (string) value of the given key.
	 * An empty string indicates not found.
	 */
	virtual std::string get(uint64_t key) = 0;

	/**
	 * Delete the given key-value pair if it exists.
	 * Returns false iff the key is not found.
	 */
	virtual bool del(uint64_t key) = 0;

	/**
	 * This resets the kvstore. All key-value pairs should be removed,
	 * including memtable and all sstables files.
	 */
	virtual void reset() = 0;

	/**
	 * Return a list including all the key-value pair between key1 and key2.
	 * keys in the list should be in an ascending order.
	 * An empty string indicates not found.
	 */
	virtual void scan(uint64_t key1, uint64_t key2, std::list<std::pair<uint64_t, std::string>>& list) = 0;

	/**
	 * This reclaims space from vLog by moving valid value and discarding invalid value.
	 * chunk_size is the _size in byte you should AT LEAST recycle.
	 */
	virtual void gc(uint64_t chunk_size) = 0;
};
