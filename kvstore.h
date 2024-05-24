#pragma once

#include <utility>
#include <thread>
#include <mutex>
#include <future>

#include "kvstore_api.h"
#include "src/include/memtable.h"
#include "src/include/version.h"
#include "src/include/keycache.h"
#include "src/include/cache.h"
#include "test/performance.h"
#include "src/include/scheduler.hpp"
#include "src/include/builder.h"

//#define MEM_MAX_SIZE 408

class KVStore : public KVStoreAPI {
    // You can add your implementation here
private:
//	struct Deleter{
//		void operator()(LSMKV::MemTable* memtable) const {
//			if (memtable->memoryUsage() != 0) {
//				callback();
//			}
//			delete memtable;
//		}
//		Deleter(std::function<void(void)>&& callback):callback(std::move(callback)){}
//		std::function<void(void)> callback;
//	};
//    std::function<void(void)> callback= [this] {KVStore::writeLevel0(this); };
//    Deleter deleter;
//	std::future<void> future;
    LSMKV::Scheduler scheduler_;
    LSMKV::Version *v;
    std::string dbname;
    std::string vlog_path;
    std::optional<std::future<void>> future_;
    LSMKV::Builder *builder_;
    LSMKV::KeyCache *kc;
    LSMKV::Cache *cache;
    Performance *p;
    static constexpr int MEM_MAX_SIZE = LSMKV::Option::pair_size_;
    const bool crc_check = true;


    std::unique_ptr<LSMKV::MemTable> mem;

    std::unique_ptr<LSMKV::MemTable> imm;

    void genBuilder();

    static void writeLevel0(KVStore *kvStore);

    void putWhenGc(uint64_t key, const LSMKV::Slice &s);

    bool GetOffset(uint64_t key, uint64_t &offset);

    int writeLevel0Table(LSMKV::MemTable *memTable);

public:
    KVStore(const std::string &dir, const std::string &vlog);
  KVStore();

  ~KVStore();

    void put(uint64_t key, const std::string &s) override;

    std::string get(uint64_t key) override;

    bool del(uint64_t key) override;

    void reset() override;

    void scan(uint64_t key1, uint64_t key2, std::list<std::pair<uint64_t, std::string>> &list) override;

    void gc(uint64_t chunk_size) override;

};
