#include "kvstore.h"
#include "src/include/builder.h"
#include <memory>
#include <string>
#include <utility>

//#define time_perf
#define MB (1024 * 1024)

void KVStore::writeLevel0(KVStore *kvStore) {
    std::unique_ptr<LSMKV::MemTable> imm;
    imm = std::move(kvStore->mem);
    kvStore->writeLevel0Table(imm.get());
}

KVStore::KVStore(const std::string &dir, const std::string &vlog)
        : KVStoreAPI(dir, vlog), v(new LSMKV::Version(dir)), dbname(dir), vlog_path(vlog) {
    p = new Performance(dir);
    p->StartTest("TOTAL");
    future_ = std::nullopt;
    kc = new LSMKV::KeyCache(dir, v);
    cache = new LSMKV::Cache();
    mem = std::make_unique<LSMKV::MemTable>();
    builder_ = new LSMKV::Builder(dbname.c_str(), v, kc);
}

static inline bool CheckCrc(const char *data, uint32_t len) {
    return data[0] == '\377' && utils::crc16(data + 3, len - 3) == LSMKV::DecodeFixed16(data + 1);
}

KVStore::~KVStore() {
    if (future_.has_value()) {
        future_->get();
        delete builder_->it_;
    }
    if (mem != nullptr && mem->memoryUsage() != 0) {
        writeLevel0Table(mem.get());
    }

    p->EndTest("TOTAL");
    delete builder_;
    delete v;
    delete cache;
    delete kc;
    delete p;
}

void KVStore::genBuilder() {
    imm = std::move(mem);
    LSMKV::Iterator *iter = imm->newIterator();
    builder_->setAll(imm->memoryUsage(), iter);
    mem = std::make_unique<LSMKV::MemTable>();
}

void KVStore::putWhenGc(uint64_t key, const LSMKV::Slice &s) {
#ifdef time_perf
    PerformanceGuard guard(p, "PUT");
#endif
    if (mem->memoryUsage() == MEM_MAX_SIZE) {
        if (future_.has_value()) {
            future_->get();
            future_ = std::nullopt;
            imm = nullptr;
            delete builder_->it_;
        }
        writeLevel0Table(mem.get());
        mem = std::make_unique<LSMKV::MemTable>();
    }
    mem->put(key, s);
}

/**
 * Insert/Update the key-value pair.
 * No return values for simplicity.
 */
void KVStore::put(uint64_t key, const std::string &s) {
#ifdef time_perf
    PerformanceGuard guard(p, "PUT");
#endif
    if (mem->memoryUsage() == MEM_MAX_SIZE) {
        if (future_.has_value()) {
            future_->get();
            imm = nullptr;
            delete builder_->it_;
        }

        std::promise<void> promise;
        future_ = promise.get_future();
        genBuilder();
        cache->Drop();
        scheduler_.Schedule({*builder_, std::move(promise)});
    }
    mem->put(key, s);
}

/**
 * Returns the (string) value of the given key.
 * An empty string indicates not found.
 */
std::string KVStore::get(uint64_t key) {
#ifdef time_perf
    PerformanceGuard guard(p, "GET");
#endif
    std::string s = std::move(mem->get(key));
    if (s.empty()) {
        if (future_.has_value()) {
            s = std::move(imm->get(key));
            if (!s.empty()) {
                if (mem->DELETED(s)) {
                    return {};
                }
                return s;
            }
            future_->get();
            future_ = std::nullopt;
            imm = nullptr;
            delete builder_->it_;
        }

        if (kc->empty()) {
            return {};
        }
        s = std::move(kc->get(key));

        if (s.empty()) {
            return {};
        }
        LSMKV::Slice result;
        uint32_t len = LSMKV::DecodeFixed32(s.data() + 8);
        if (len == 0) {
            return {};
        }

        const char *data;
        std::unique_ptr<char[]> buf;
        len += 15;

        if (len >= 2 * MB) {
            buf = std::make_unique<char[]>(len);
            LSMKV::RandomReadableFile *file;
            LSMKV::NewRandomReadableFile(LSMKV::VLogFileName(dbname), &file);
            file->Read(LSMKV::DecodeFixed64(s.data()), len, &result, buf.get());
            delete file;
        } else [[likely]] {
            auto offset = LSMKV::DecodeFixed64(s.data());
            if (!cache->Get(offset, len, result)) {
                cache->Drop();
                LSMKV::MemoryReadableFile *file;
                if (!LSMKV::NewRandomMemoryReadFile(LSMKV::VLogFileName(dbname), 4 * MB, offset, &file)) {
                    buf = std::make_unique<char[]>(len);
                    LSMKV::RandomReadableFile *file_;
                    LSMKV::NewRandomReadableFile(LSMKV::VLogFileName(dbname), &file_);
                    file_->Read(LSMKV::DecodeFixed64(s.data()), len, &result, buf.get());
                    delete file;
                } else {
                    cache->Push(file);
                    cache->Get(offset, len, result);
                }
            }
        }

        data = result.data();

        if (result.size() <= 15 ||
            (crc_check && !CheckCrc(data, len))) [[unlikely]] {
            return {};
        }
        s.reserve(len -= 15);
        s.resize(len);
        LSMKV::memcpy_tiny(s.data(), data + 15, len);
        return std::move(s);
    }
    if (mem->DELETED(s)) {
        return {};
    }
    return s;
}

/**
 * Delete the given key-value pair if it exists.
 * Returns false iff the key is not found.
 */
bool KVStore::del(uint64_t key) {
#ifdef time_perf
    PerformanceGuard guard(p, "DEL");
#endif
    std::string s = std::move(get(key));
    if (!s.empty() && !mem->DELETED(s)) {
        put(key, "~DELETED~");
        return true;
    }
    return false;
}

/**
 * This resets the kvstore. All key-value pairs should be removed,
 * including memtable and all sstables files.
 */
void KVStore::reset() {
    PerformanceGuard guard(p, "RESET");
    if (future_.has_value()) {
        future_->get();
        future_ = std::nullopt;
        delete builder_->it_;
    }

    imm = nullptr;
    cache->Drop();

    utils::rmfiles(dbname);
    v->reset();
    kc->reset();
    mem = std::make_unique<LSMKV::MemTable>();
}

/**
 * Return a list including all the key-value pair between key1 and key2.
 * keys in the list should be in an ascending order.
 * An empty string indicates not found.
 */
void KVStore::scan(uint64_t key1, uint64_t key2, std::list<std::pair<uint64_t, std::string>> &list) {
#ifdef time_perf
    PerformanceGuard guard(p, "SCAN");
#endif

    LSMKV::Iterator *iter = mem->newIterator();
    std::list<std::pair<uint64_t, std::string>> tmp_list;
    iter->scan(key1, key2, tmp_list);
    if (future_.has_value()) {
        future_->get();
        future_ = std::nullopt;
        imm = nullptr;
        delete builder_->it_;
    }

    std::map<uint64_t, std::string> map;
    kc->scan(key1, key2, map);

    LSMKV::RandomReadableFile *files;
    char buf[1024];

    LSMKV::NewRandomReadableFile(LSMKV::VLogFileName(dbname), &files);
    std::unique_ptr<LSMKV::RandomReadableFile> file(files);
    for (auto &it: map) {
        LSMKV::Slice result;
        uint32_t len = LSMKV::DecodeFixed32(it.second.data() + 8) + 15;
        if (len <= 1024) {
            file->Read(LSMKV::DecodeFixed64(it.second.data()), len, &result, buf);
            if (crc_check && !CheckCrc(result.data(), len)) [[unlikely]] {
                continue;
            }
            result.remove_prefix(15);
            it.second = std::move(result.toString());
        } else {
            std::unique_ptr<char[]> large_buf = std::make_unique<char[]>(len);
            file->Read(LSMKV::DecodeFixed64(it.second.data()), len, &result, large_buf.get());
            if (crc_check && !CheckCrc(result.data(), len)) [[unlikely]] {
                continue;
            }
            result.remove_prefix(15);
            it.second = std::move(result.toString());
        }
    }

    map.insert(tmp_list.begin(), tmp_list.end());
    for (auto &it: map) {
        if (!mem->DELETED(it.second)) {
            list.emplace_back(it);
        }
    }
    delete iter;
}

bool KVStore::GetOffset(uint64_t key, uint64_t &offset) {
    if (!mem->contains(key)) {
        return kc->GetOffset(key, offset);
    }
    return false;
}


/**
 * This reclaims space from vLog by moving valid value and discarding invalid value.
 * chunk_size is the _size in byte you should AT LEAST recycle.
 */
void KVStore::gc(uint64_t chunk_size) {
#ifdef time_perf
    PerformanceGuard guard(p, "GC");
#endif
    if (future_.has_value()) {
        future_->get();
        future_ = std::nullopt;
        imm = nullptr;
        delete builder_->it_;
    }
    cache->Drop();
    LSMKV::RandomReadableFile *files;
    LSMKV::NewRandomReadableFile(vlog_path, &(files));
    std::unique_ptr<LSMKV::RandomReadableFile> file(files);

    // TODO NOT OVERFLOW
    LSMKV::Slice result, value;

    int factor = chunk_size * 2 < chunk_size ? 1 : 2;
    uint64_t current_size = 0;
    uint64_t vlen = 0;
    uint64_t offset = 0;
    uint32_t len;
    uint64_t key;
    char *ptr;
    uint64_t _chunk_size = chunk_size * factor;
    std::string value_buf;
    std::unique_ptr<char []> tmp;
    tmp = std::make_unique<char[]>(_chunk_size);
//    char *tmp = new char[_chunk_size];
//    uint64_t new_offset = v->head;

    // I FORGET TO WRITE COMMENT!
    // NOW I DON'T KNOW HOW I DO THIS

    while (current_size < chunk_size) {
        // value_buf is the start of ceil piece of value
        if (!value_buf.empty()) {
            tmp = std::make_unique<char[]>(vlen);
            file->Read(v->tail + _chunk_size, vlen, &result, tmp.get());
//            assert(result.size() == vlen);
        } else {
            // READ A _CHUNK_SIZE(ALWAYS TWICE THAN _CHUNK_SIZE)
            file->Read(v->tail, _chunk_size, &result, tmp.get());
            _chunk_size = result.size();
            if (_chunk_size == 0) {
                break;
            }
        }
        while (current_size < chunk_size) {
            if (!value_buf.empty()) {
                // APPEND THE NEXT PART OF CEIL PIECE
                value_buf.append(tmp.get(), vlen);
                ptr = &value_buf[0];
                len = value_buf.size() - 15;
                assert(len == LSMKV::DecodeFixed64(ptr + 3));
                key = LSMKV::DecodeFixed64(&value_buf[3]);
            } else {
                // CURRENT PTR IN TMP
                ptr = current_size + tmp.get();

                // TODO WHEN FACTOR == 1 , COULDN'T READ THE LEN
                // THE LEN OF VALUE
                len = LSMKV::DecodeFixed32(ptr + 11);
                // THE KEY OF VALUE
                key = LSMKV::DecodeFixed64(ptr + 3);

                // CANT FETCH ALL BYTES IN _CHUNK_SIZE
                if (len + current_size + 15 > _chunk_size) {
                    // vlen is the remaining part length
                    vlen = len + current_size + 15 - _chunk_size;
                    // STORE THE FIRST PART OF CEIL PIECE
                    value_buf.append(ptr, len + 15 - vlen);
                    break;
                }
            }
            // DO CRC CHECK AND CHECK WHERE IT IS THE NEWEST VALUE
            if (CheckCrc(ptr, len + 15) && GetOffset(key, offset) && (offset == v->tail + current_size)) {
                value = LSMKV::Slice(ptr + 15, len);
                putWhenGc(key, value);
            }
            current_size += len + 15;
        }
    }
    assert(current_size <= INT64_MAX);
    assert(v->tail <= INT64_MAX);
#ifdef time_perf
    PerformanceGuard guard_(p, "dig hole");
#endif
    assert(current_size < INT64_MAX);
    assert(v->tail < INT64_MAX);
    if (current_size != 0) {
        int st = utils::de_alloc_file(vlog_path, v->tail, current_size);
        assert(st == 0);
    }
#ifdef time_perf
    guard_.drop();
#endif

    v->tail += current_size;
    if (mem->memoryUsage() != 0) {
        if (future_.has_value()) {

            future_->get();
            delete builder_->it_;
            imm = nullptr;
        }

        std::promise<void> promise;
        future_ = promise.get_future();
        genBuilder();
        cache->Drop();
        scheduler_.Schedule({*builder_, std::move(promise)});
    }
}

int KVStore::writeLevel0Table(LSMKV::MemTable *memtable) {
#ifdef time_perf
    PerformanceGuard guard_(p, "WRITE");
#endif
    LSMKV::Iterator *iter = memtable->newIterator();
    BuildTable(dbname, v, iter, memtable->memoryUsage(), kc);
    delete iter;
    return 0;
}
