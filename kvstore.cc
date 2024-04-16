#include "kvstore.h"
#include "src/builder.h"
#include <memory>
#include <string>

static inline int
writeLevel0Table(const std::string &dbname, LSMKV::MemTable *memtable, LSMKV::Version *v, LSMKV::KeyCache *kc) {
    LSMKV::Iterator *iter = memtable->newIterator();
    FileMeta meta;
    meta.size = memtable->memoryUsage();
    BuildTable(dbname, v, iter, meta, kc);
    delete iter;
    return 0;
}

void
KVStore::writeLevel0(const std::string &dbname, LSMKV::MemTable *memtable, LSMKV::Version *v, LSMKV::KeyCache *kc) {
    std::unique_ptr<LSMKV::MemTable> imm;
    imm.reset(memtable);
//	kvStore->writeLevel0Table(imm.get());
    ::writeLevel0Table(dbname, imm.get(), v, kc);
}

KVStore::KVStore(const std::string &dir, const std::string &vlog)
        : KVStoreAPI(dir, vlog), v(new LSMKV::Version(dir)), dbname(dir), vlog_path(vlog),
          deleter(std::move(callback)), mem(new LSMKV::MemTable(), deleter) {
    kc = new LSMKV::KeyCache(dir, v);
    p = new Performance(dir);
    p->StartTest("TOTAL");
//	cache = LSMKV::NewLRUCache(100);
}

KVStore::~KVStore() {
    if (future.valid()) {
        future.get();
    }
    delete mem.release();
    p->EndTest("TOTAL");
    delete p;
    delete v;
//	delete cache;
    delete kc;
}

void KVStore::putWhenGc(uint64_t key, const LSMKV::Slice &s) {
    p->StartTest("PUT");
    if (mem->memoryUsage() == MEM_MAX_SIZE) {
        if (future.valid()) {
            future.get();
        }
        future = std::async(std::launch::async, writeLevel0, dbname, mem.release(), v, kc);
        mem.reset(new LSMKV::MemTable());
//		writeLevel0(this);
//		mem.reset(new LSMKV::MemTable());SS
//		std::thread t(writeLevel0,this);
//		t.detach();
//		if (imm == nullptr) {
//			imm.reset(mem.release());
//			writeLevel0Table(imm.get());
//			imm = nullptr;
//            mem.reset(new LSMKV::MemTable());
//		} else {
//			auto ptr = mem.release();
//			writeLevel0Table(ptr);
//			delete ptr;
//			mem.reset(new LSMKV::MemTable());
//		}
    }
    mem->put(key, s);
    p->EndTest("PUT");
}

/**
 * Insert/Update the key-value pair.
 * No return values for simplicity.
 */
void KVStore::put(uint64_t key, const std::string &s) {
    p->StartTest("PUT");
    if (mem->memoryUsage() == MEM_MAX_SIZE) {
        if (future.valid()) {
            future.get();
        }
        future = std::async(std::launch::async, writeLevel0, dbname, mem.release(), v, kc);
        mem.reset(new LSMKV::MemTable());
//		writeLevel0(this);
//		mem.reset(new LSMKV::MemTable());
//		std::thread t(writeLevel0,this);
//		t.detach();
//		std::thread t(writeLevel0,this);
//		t.detach();
//		mem.reset(new LSMKV::MemTable());
//		if (imm == nullptr) {
//			imm.reset(mem.release());
//			mem.reset(new LSMKV::MemTable());
//			writeLevel0Table(imm.get());
//			imm = nullptr;
//		} else {
//			auto ptr = mem.release();
//			writeLevel0Table(ptr);
//			delete ptr;
//			mem.reset(new LSMKV::MemTable());
//		}
    }
    mem->put(key, s);
    p->EndTest("PUT");
}

static inline bool CheckCrc(const char *data, uint32_t len) {
    return data[0] == '\377' && utils::crc16(data + 3, len - 3) == LSMKV::DecodeFixed16(data + 1);
}

/**
 * Returns the (string) value of the given key.
 * An empty string indicates not found.
 */
std::string KVStore::get(uint64_t key) {
    p->StartTest("GET");
    std::string s = mem->get(key);
    if (s.empty()) {
        //TODO NEED TO OPTIMIZE
        if (future.valid()) {
            future.get();
        }
        if (kc->empty()) {
            p->EndTest("GET");
            return "";
        }

        s = kc->get(key);
        if (s.empty()) {
            p->EndTest("GET");
            return "";
        }

        LSMKV::Slice result;
        uint32_t len = LSMKV::DecodeFixed32(s.data() + 8);
        if (len == 0) {
            p->EndTest("GET");
            return "";
        }

        char buf[len + 15];
        LSMKV::RandomReadableFile *file;
        LSMKV::NewRandomReadableFile(LSMKV::VLogFileName(dbname), &file);
        file->Read(LSMKV::DecodeFixed64(s.data()), len + 15, &result, buf);
        delete file;
        p->EndTest("GET");
        if (result.size() <= 15 || !CheckCrc(result.data(), len + 15)) {
            assert(false);
        }
        return {result.data() + 15, result.size() - 15};
    }
    if (mem->DELETED(s)) {
        p->EndTest("GET");

        return "";
    }
    p->EndTest("GET");

    return s;
}

/**
 * Delete the given key-value pair if it exists.
 * Returns false iff the key is not found.
 */
bool KVStore::del(uint64_t key) {
    p->StartTest("DEL");
    std::string s = get(key);
    if (!s.empty() && !mem->DELETED(s)) {
        put(key, "~DELETED~");
        return true;
    }
    p->EndTest("DEL");
    return false;
}

/**
 * This resets the kvstore. All key-value pairs should be removed,
 * including memtable and all sstables files.
 */
void KVStore::reset() {
//	imm = nullptr;
    delete mem.release();

    // Otherwise it will destructor twice!!!
//	imm = nullptr;
    if (future.valid()) {
        future.get();
    }
    utils::rmfiles(dbname);
    v->reset();
    kc->reset();
    mem.reset(new LSMKV::MemTable());
}

/**
 * Return a list including all the key-value pair between key1 and key2.
 * keys in the list should be in an ascending order.
 * An empty string indicates not found.
 */
void KVStore::scan(uint64_t key1, uint64_t key2, std::list<std::pair<uint64_t, std::string>> &list) {
    p->StartTest("SCAN");
    std::map<uint64_t, std::string> map;
    if (future.valid()) {
        future.get();
    }
    kc->scan(key1, key2, map);
    LSMKV::RandomReadableFile *file;
    for (auto &it: map) {
        LSMKV::Slice result;
        uint32_t len = LSMKV::DecodeFixed32(it.second.data() + 8);
        char buf[len + 15];
        LSMKV::NewRandomReadableFile(LSMKV::VLogFileName(dbname), &file);
        file->Read(LSMKV::DecodeFixed64(it.second.data()), len + 15, &result, buf);
        delete file;
        if (!CheckCrc(result.data(), len + 15)) {
            continue;
        }
        result.remove_prefix(15);
        it.second = result.toString();
    }
    LSMKV::Iterator *iter = mem->newIterator();
    std::list<std::pair<uint64_t, std::string>> tmp_list;
    iter->scan(key1, key2, tmp_list);

    map.insert(tmp_list.begin(), tmp_list.end());
    for (auto &it: map) {
        if (!mem->DELETED(it.second)) {
            list.emplace_back(it);
        }
    }
    delete iter;
    p->EndTest("SCAN");
}

//void KVStore::scan(uint64_t key1, uint64_t key2, std::list<std::pair<uint64_t, std::string>>& list) {
//    p->StartTest("SCAN");
//    std::map<uint64_t, std::string> map;
//    std::map<uint64_t,std::pair<uint64_t,uint64_t>> tmp_map;
//    kc->scan(key1, key2, tmp_map);
//    LSMKV::SequentialFile *file;
//    LSMKV::NewSequentialFile(vlog_path,&file);
//    file->MoveTo(tmp_map.begin()->first);
//    LSMKV::Slice result;
//
//    for (auto& it : tmp_map) {
//        uint32_t len = it.second.second;
//        char buf[len + 15];
//        file->Read(len + 15, &result, buf);
//        //todo do check crc here
//        result.remove_prefix(15);
//        map.emplace(it.second.first, result.toString());
//    }
//    delete file;
//
//    LSMKV::Iterator* iter = mem->newIterator();
//    std::list<std::pair<uint64_t, std::string>> tmp_list;
//    iter->scan(key1, key2, tmp_list);
//    for (auto& it : tmp_list) {
//        map.emplace(it);
//    }
//    for (auto& it : map) {
//        if (!mem->DELETED(it.second)) {
//            list.emplace_back(it);
//        }
//    }
//    delete iter;
//    p->EndTest("SCAN");
//}

bool KVStore::GetOffset(uint64_t key, uint64_t &offset) {
    if (future.valid()) {
        future.get();
    }
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
    p->StartTest("GC");
    if (future.valid()) {
        future.get();
    }
    LSMKV::RandomReadableFile *file;
    LSMKV::Status s = LSMKV::NewRandomReadableFile(vlog_path, &file);
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
    char *tmp = new char[_chunk_size];
    uint64_t new_offset = v->head;

    // I FORGET TO WRITE COMMENT!
    // NOW I DON'T KNOW HOW I DO THIS

    while (current_size < chunk_size) {
        // value_buf is the start of ceil piece of value
        if (!value_buf.empty()) {
            tmp = new char[vlen];
            file->Read(v->tail + _chunk_size, vlen, &result, tmp);
            assert(result.size() == vlen);
        } else {
            p->StartTest("read vlog");
            // READ A _CHUNK_SIZE(ALWAYS TWICE THAN _CHUNK_SIZE)
            file->Read(v->tail, _chunk_size, &result, tmp);
            _chunk_size = result.size();
            p->EndTest("read vlog");
        }
        p->StartTest("start find key");
        while (current_size < chunk_size) {
            if (!value_buf.empty()) {
                // APPEND THE NEXT PART OF CEIL PIECE
                value_buf.append(tmp, vlen);
                ptr = &value_buf[0];
                len = value_buf.size() - 15;
                assert(len == LSMKV::DecodeFixed64(ptr + 3));
                key = LSMKV::DecodeFixed64(&value_buf[3]);
            } else {
                // CURRENT PTR IN TMP
                ptr = current_size + tmp;

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
            if (key == 15312) {
                int h = 7;
            }
            // DO CRC CHECK AND CHECK WHERE IT IS THE NEWEST VALUE
            if (CheckCrc(ptr, len + 15) && GetOffset(key, offset) && (offset == v->tail + current_size)) {
                value = LSMKV::Slice(ptr + 15, len);
                //cache->Insert(new_offset, value);
//                put(value, key);
                putWhenGc(key, value);
//                putWhenGc(key, value);
                // MAYBE USE TO BUILD THE VALUE_CACHE
                // new_offset += len + 15;
            }
            current_size += len + 15;
        }
        delete file;
        delete[] tmp;
    }
    p->EndTest("start find key");
    assert(current_size <= INT64_MAX);
    assert(v->tail <= INT64_MAX);
    p->StartTest("dig hole");
    int st = utils::de_alloc_file(vlog_path, v->tail, current_size);
    p->EndTest("dig hole");
    assert(st == 0);
    v->tail += current_size;
    if (mem->memoryUsage() != 0) {
//		auto m = mem.release();
//		writeLevel0Table(m);
//		mem.reset(new LSMKV::MemTable());
        if (future.valid()) {
            future.get();
        }
        future = std::async(std::launch::async, writeLevel0, dbname, mem.release(), v, kc);
        mem.reset(new LSMKV::MemTable());
    }
    p->EndTest("GC");
}

void KVStore::writeLevel0Table(LSMKV::MemTable *memtable) {
    p->StartTest("WRITE");
    ::writeLevel0Table(dbname, memtable, v, kc);
    p->EndTest("WRITE");
}
