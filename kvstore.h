#pragma once

#include <utility>

#include "kvstore_api.h"
#include "src/memtable.h"
#include "src/version.h"
#include "src/keycache.h"
#include "src/cache.h"
#include "test/performance.h"

#define MEM_MAX_SIZE 408

class KVStore : public KVStoreAPI {
	// You can add your implementation here
private:
	struct Deleter{
		void operator()(LSMKV::MemTable* memtable) const {
            std::cout<<"deleter"<<std::endl;
			if (memtable->memoryUsage() != 0) {
				callback();
			}
			delete memtable;
		}
		Deleter(std::function<int(void)>&& callback):callback(std::move(callback)){}
		std::function<int(void)> callback;
	};
	LSMKV::Version* v;
	std::string dbname;
	std::string vlog_path;
	LSMKV::KeyCache* kc;
	LSMKV::Cache* cache;
	Performance* p;
	std::function<int(void)> callback= [this] {return KVStore::writeLevel0(this); };
	Deleter deleter;
	std::unique_ptr<LSMKV::MemTable,Deleter> mem;
	std::unique_ptr<LSMKV::MemTable> imm;


	static int writeLevel0(KVStore *kvStore);

	void putWhenGc(uint64_t key, const LSMKV::Slice& s);

	bool GetOffset(uint64_t key, uint64_t& offset);

	int writeLevel0Table(LSMKV::MemTable* memTable);
public:
	KVStore(const std::string& dir, const std::string& vlog);

	~KVStore();

	void put(uint64_t key, const std::string& s) override;

	std::string get(uint64_t key) override;

	bool del(uint64_t key) override;

	void reset() override;

	void scan(uint64_t key1, uint64_t key2, std::list<std::pair<uint64_t, std::string>>& list) override;

	void gc(uint64_t chunk_size) override;

};
