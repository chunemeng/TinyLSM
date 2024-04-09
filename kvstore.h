#pragma once

#include "kvstore_api.h"
#include "src/memtable.h"
#include "src/version.h"
#include "src/keycache.h"
#include "src/cache.h"
#include "test/performance.h"


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
	LSMKV::Cache* cache;
    Performance* p;
public:
	KVStore(const std::string &dir, const std::string& vlog);

	~KVStore();

	void put(uint64_t key, const std::string &s) override;

    void put(const LSMKV::Slice& s, uint64_t key);

    void putWhenGc(uint64_t key,const LSMKV::Slice &s);

	std::string get(uint64_t key) override;

	bool del(uint64_t key) override;

	void reset() override;

	void scan(uint64_t key1, uint64_t key2, std::list<std::pair<uint64_t, std::string>> &list) override;

	void gc(uint64_t chunk_size) override;

    bool GetOffset(uint64_t key, uint64_t& offset);

	int writeLevel0Table(LSMKV::MemTable* memTable);
};
