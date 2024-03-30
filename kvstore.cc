#include "kvstore.h"
#include "src/builder.h"
#include <string>
#include <iostream>
#include <zlib.h>

KVStore::KVStore(const std::string& dir, const std::string& vlog)
	: KVStoreAPI(dir, vlog), mem(new LSMKV::MemTable()), v(new LSMKV::Version(dir)),
	  dbname(dir), vlog_path(vlog) {
	kc = new LSMKV::KeyCache(dir);
	cache = LSMKV::NewLRUCache(100);
}

KVStore::~KVStore() {
	if (mem->memoryUsage() != 0) {
		writeLevel0Table(mem);
	} else {
		delete mem;
	}
	if (!imm) {
		delete imm;
	}
	delete v;
	delete cache;
	delete kc;
}

/**
 * Insert/Update the key-value pair.
 * No return values for simplicity.
 */
void KVStore::put(uint64_t key, const std::string& s) {
	if (mem->memoryUsage() == MEM_MAX_SIZE) {
		if (imm == nullptr) {
			imm = mem;
			mem = new LSMKV::MemTable();
			writeLevel0Table(imm);
			imm = nullptr;
		} else {
			writeLevel0Table(mem);
			mem = new LSMKV::MemTable();
		}
	}
	mem->put(key, s);
}
/**
 * Returns the (string) value of the given key.
 * An empty string indicates not found.
 */
std::string KVStore::get(uint64_t key) {
	std::string s = mem->get(key);
	if (s.empty()) {
		//TODO NEED TO OPTIMIZE
		if (kc->empty()) {
			return "";
		}
		s = kc->get(key);
		if (s.empty() || mem->DELETED(s)) {
			return "";
		}
		LSMKV::Slice result;
		uint32_t len = LSMKV::DecodeFixed32(s.data() + 8);
		if (len == 0) {
			return "";
		}
		char buf[len + 15];
		LSMKV::RandomReadableFile* file;
		LSMKV::NewRandomReadableFile(LSMKV::VLogFileName(dbname), &file);
		file->Read(LSMKV::DecodeFixed64(s.data()), len + 15, &result, buf);
		delete file;
		return { result.data() + 15, result.size() - 15 };
	}
	if (mem->DELETED(s)) {
		return "";
	}
	return s;
}
/**
 * Delete the given key-value pair if it exists.
 * Returns false iff the key is not found.
 */
bool KVStore::del(uint64_t key) {
	std::string s = get(key);
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
	delete mem;
	delete imm;

	// Otherwise it will destructor twice!!!
	imm = nullptr;
	utils::rmfiles(dbname);
	v->reset();
	kc->reset();
	mem = new LSMKV::MemTable();
}

/**
 * Return a list including all the key-value pair between key1 and key2.
 * keys in the list should be in an ascending order.
 * An empty string indicates not found.
 */
void KVStore::scan(uint64_t key1, uint64_t key2, std::list<std::pair<uint64_t, std::string>>& list) {
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
		list.emplace_back(it);
	}
	delete iter;
}

bool CheckCrc(const char* data, uint32_t len) {
	return data[0] == '\377' && utils::crc16(data + 3, len) == LSMKV::DecodeFixed16(data + 1);
}

/**
 * This reclaims space from vLog by moving valid value and discarding invalid value.
 * chunk_size is the _size in byte you should AT LEAST recycle.
 */
void KVStore::gc(uint64_t chunk_size) {
	LSMKV::SequentialFile* file;
	LSMKV::NewSequentialFile(vlog_path, &file);
	file->Skip(v->tail);
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

	while (current_size < chunk_size) {
		if (!value_buf.empty()) {
			tmp = new char[vlen];
			file->Read(vlen, &result, tmp);
		} else {
			file->Read(_chunk_size, &result, tmp);
		}
		while (current_size < chunk_size) {
			if (!value_buf.empty()) {
				value_buf.append(tmp, vlen);
				ptr = &value_buf[0];
				len = vlen;
				key = LSMKV::DecodeFixed32(&value_buf[3]);
			} else {
				ptr = current_size + tmp;
				len = LSMKV::DecodeFixed32(ptr + 11);
				key = LSMKV::DecodeFixed32(ptr + 3);

				// NOT FETCH ALL BYTES
				if (len + current_size + 15 > _chunk_size) {
					vlen = len + current_size + 15 > _chunk_size;
					value_buf.append(ptr, len + 15);
					break;
				}

			}
			if (CheckCrc(ptr, len + 12) && kc->GetOffset(key, offset) && offset == v->tail + current_size) {
				value = LSMKV::Slice(ptr + 15, len);
				cache->Insert(new_offset, value);
				mem->put(key, value);
				new_offset += len + 15;
			}
			current_size += len + 15;
		}
	}

	utils::de_alloc_file(vlog_path, v->tail, current_size);
	v->tail += current_size;
	writeLevel0Table(mem);
	mem = new LSMKV::MemTable();
}

int KVStore::writeLevel0Table(LSMKV::MemTable* memtable) {
	LSMKV::Iterator* iter = memtable->newIterator();
	FileMeta meta;
	meta.size = memtable->memoryUsage();
	BuildTable(dbname, v, iter, meta, kc);
	delete iter;
	delete memtable;
	return 0;
}
