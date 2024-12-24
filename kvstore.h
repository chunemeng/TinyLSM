#pragma once

#include <future>
#include <mutex>
#include <thread>
#include <utility>

#include "include/builder.h"
#include "include/cache.h"
#include "include/keycache.h"
#include "include/memtable.h"
#include "include/performance.h"
#include "include/scheduler.hpp"
#include "include/version.h"
#include "kvstore_api.h"

class KVStore : public KVStoreAPI {
private:
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
    static constexpr bool crc_check = true;


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
