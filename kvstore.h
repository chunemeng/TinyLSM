#pragma once

#include "kvstore_api.h"
#include "src/memtable.h"
#include "src/version.h"
#include "src/keycache.h"

#define MEM_MAX_SIZE 408


class KVStore : public KVStoreAPI
{
	// You can add your implementation here
private:
	LSMKV::MemTable* mem;
	LSMKV::MemTable* imm;
	LSMKV::Version* v;
	std::string dbname;
	std::string vlog_path;
	LSMKV::KeyCache* kc;

public:
	KVStore(const std::string &dir, const std::string& vlog);

	~KVStore();

	void put(uint64_t key, const std::string &s) override;

	std::string get(uint64_t key) override;

	bool del(uint64_t key) override;

	void reset() override;

	void scan(uint64_t key1, uint64_t key2, std::list<std::pair<uint64_t, std::string>> &list) override;

	void gc(uint64_t chunk_size) override;

	int writeLevel0Table(LSMKV::MemTable* memTable);
};
