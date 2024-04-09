#include "kvstore.h"
#include "src/builder.h"
#include <memory>
#include <string>
#include <iostream>

int KVStore::writeLevel0(KVStore* kvStore) {
	std::unique_ptr<LSMKV::MemTable> imm;
	imm.reset(kvStore->mem.release());
	return kvStore->writeLevel0Table(imm.get());
}

KVStore::KVStore(const std::string& dir, const std::string& vlog)
	: KVStoreAPI(dir, vlog), v(new LSMKV::Version(dir)), dbname(dir), vlog_path(vlog),
	  deleter(std::move(callback)), mem(nullptr, deleter) {
	kc = new LSMKV::KeyCache(dir, v);
	p = new Performance(dir);
//	cache = LSMKV::NewLRUCache(100);
}

KVStore::~KVStore() {
	delete p;
	delete v;
//	delete cache;
	delete kc;
}

void KVStore::putWhenGc(uint64_t key, const LSMKV::Slice& s) {
	p->StartTest("PUT");
	if (mem->memoryUsage() == MEM_MAX_SIZE) {
		if (imm == nullptr) {
			imm.reset(mem.release());
			writeLevel0Table(imm.get());
			imm = nullptr;
		} else {
			auto ptr = mem.release();
			writeLevel0Table(ptr);
			delete ptr;
			mem.reset(new LSMKV::MemTable());
		}
	}
	mem->put(key, s);
	p->EndTest("PUT");
}

/**
 * Insert/Update the key-value pair.
 * No return values for simplicity.
 */
void KVStore::put(uint64_t key, const std::string& s) {
	p->StartTest("PUT");
	if (mem->memoryUsage() == MEM_MAX_SIZE) {
		if (imm == nullptr) {
			imm.reset(mem.release());
			mem.reset(new LSMKV::MemTable());
			writeLevel0Table(imm.get());
			imm = nullptr;
		} else {
			auto ptr = mem.release();
			writeLevel0Table(ptr);
			delete ptr;
			mem.reset(new LSMKV::MemTable());
		}
	}
	mem->put(key, s);
	p->EndTest("PUT");
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
		LSMKV::RandomReadableFile* file;
		LSMKV::NewRandomReadableFile(LSMKV::VLogFileName(dbname), &file);
		file->Read(LSMKV::DecodeFixed64(s.data()), len + 15, &result, buf);
		delete file;
		p->EndTest("GET");
		if (result.size() <= 15) {
			return {};
		}
		return { result.data() + 15, result.size() - 15 };
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
	imm = nullptr;
	delete mem.release();

	// Otherwise it will destructor twice!!!
	imm = nullptr;
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
void KVStore::scan(uint64_t key1, uint64_t key2, std::list<std::pair<uint64_t, std::string>>& list) {
	p->StartTest("SCAN");
	std::map<uint64_t, std::string> map;
	kc->scan(key1, key2, map);
	for (auto& it : map) {
		LSMKV::Slice result;
		uint32_t len = LSMKV::DecodeFixed32(it.second.data() + 8);
		char buf[len + 15];
		LSMKV::RandomReadableFile* file;
		LSMKV::NewRandomReadableFile(LSMKV::VLogFileName(dbname), &file);
		file->Read(LSMKV::DecodeFixed64(it.second.data()), len + 15, &result, buf);
		delete file;
		it.second = std::string{ result.data() + 15, result.size() - 15 };
	}
	LSMKV::Iterator* iter = mem->newIterator();
	std::list<std::pair<uint64_t, std::string>> tmp_list;
	iter->scan(key1, key2, tmp_list);
	for (auto& it : tmp_list) {
		map.insert(it);
	}
	for (auto& it : map) {
		if (!mem->DELETED(it.second)) {
			list.emplace_back(it);
		}
	}
	delete iter;
	p->EndTest("SCAN");
}

bool KVStore::GetOffset(uint64_t key, uint64_t& offset) {
	if (!mem->contains(key)) {
		return kc->GetOffset(key, offset);
	}
	return false;
}

bool CheckCrc(const char* data, uint32_t len) {
	return data[0] == '\377' && utils::crc16(data + 3, len) == LSMKV::DecodeFixed16(data + 1);
}

/**
 * This reclaims space from vLog by moving valid value and discarding invalid value.
 * chunk_size is the _size in byte you should AT LEAST recycle.
 */
void KVStore::gc(uint64_t chunk_size) {
	p->StartTest("GC");
	LSMKV::RandomReadableFile* file;
	LSMKV::Status s = LSMKV::NewRandomReadableFile(vlog_path, &file);
	// TODO NOT OVERFLOW
	LSMKV::Slice result, value;

	int factor = chunk_size * 2 < chunk_size ? 1 : 2;
	uint64_t current_size = 0;
	uint64_t vlen = 0;
	uint64_t offset = 0;
	uint32_t len;
	uint64_t key;
	char* ptr;
	uint64_t _chunk_size = chunk_size * factor;
	std::string value_buf;
	char* tmp = new char[_chunk_size];
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
			// READ A _CHUNK_SIZE(ALWAYS TWICE THAN _CHUNK_SIZE)
			file->Read(v->tail, _chunk_size, &result, tmp);
			_chunk_size = result.size();
		}

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
			// DO CRC CHECK AND CHECK WHERE IT IS THE NEWEST VALUE
			if (CheckCrc(ptr, len + 12) && GetOffset(key, offset) && (offset == v->tail + current_size)) {
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
	assert(current_size <= INT64_MAX);
	assert(v->tail <= INT64_MAX);
	utils::de_alloc_file(vlog_path, v->tail, current_size);

	v->tail += current_size;
	if (mem->memoryUsage() != 0) {
		auto m = mem.release();
		writeLevel0Table(m);
		mem.reset(new LSMKV::MemTable());
	}
	p->EndTest("GC");
}

int KVStore::writeLevel0Table(LSMKV::MemTable* memtable) {
	p->StartTest("WRITE");
	LSMKV::Iterator* iter = memtable->newIterator();
	FileMeta meta;
	meta.size = memtable->memoryUsage();
	BuildTable(dbname, v, iter, meta, kc);
	delete iter;
	p->EndTest("WRITE");
	return 0;
}
