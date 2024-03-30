#include "kvstore.h"
#include "src/builder.h"
#include <string>
#include <iostream>

KVStore::KVStore(const std::string& dir, const std::string& vlog)
	: KVStoreAPI(dir, vlog), mem(new LSMKV::MemTable()), v(new LSMKV::Version(dir)),
	  dbname(dir), vlog_path(vlog) {
	kc = new LSMKV::KeyCache(dir);
}

KVStore::~KVStore() {
	if (mem->memoryUsage() != 0) {
		writeLevel0Table(mem);
	} else {
		delete mem;
	}
	delete v;
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
		LSMKV::RandomReadableFile *file;
		LSMKV::NewRandomReadableFile(LSMKV::VLogFileName(dbname), &file);
		file->Read(LSMKV::DecodeFixed64(s.data()), len + 15,&result,buf);
		delete file;
		return {result.data() + 15, result.size() - 15};
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
	std::list<std::pair<uint64_t, std::string>> li;
	kc->scan(key1, key2, li);
	std::map<uint64_t, std::string> map;
	for (auto & it : li) {
		LSMKV::Slice result;
		uint32_t len = LSMKV::DecodeFixed32(it.second.data() + 8);
		char buf[len + 15];
		LSMKV::RandomReadableFile *file;
		LSMKV::NewRandomReadableFile(LSMKV::VLogFileName(dbname), &file);
		file->Read(LSMKV::DecodeFixed64(it.second.data()), len + 15,&result,buf);
		delete file;
		map.insert(std::make_pair(it.first,  std::string {result.data() + 15, result.size() - 15}));
	}
	LSMKV::Iterator* iter = mem->newIterator();
	std::list<std::pair<uint64_t, std::string>> tmp_list;
	iter->scan(key1, key2, tmp_list);
	for (auto & it : tmp_list) {
		map.insert(it);
	}
	for (auto & it : map) {
		list.emplace_back(it);
	}
	delete iter;
}

/**
 * This reclaims space from vLog by moving valid value and discarding invalid value.
 * chunk_size is the _size in byte you should AT LEAST recycle.
 */
void KVStore::gc(uint64_t chunk_size) {
	LSMKV::SequentialFile *file;
	LSMKV::NewSequentialFile(vlog_path,&file);
	file->Skip(v->tail);
	// TODO NOT OVERFLOW
	LSMKV::Slice result;
	char tmp[2 * chunk_size];
	file->Read(chunk_size * 2, &result,tmp);
	utils::de_alloc_file(vlog_path ,v->tail, chunk_size);
}




int KVStore::writeLevel0Table(LSMKV::MemTable* memtable) {
	LSMKV::Iterator* iter = memtable->newIterator();
	FileMeta meta;
	meta.size = memtable->memoryUsage();
	BuildTable(dbname, v, iter,meta,kc);
	delete iter;
	delete memtable;
	return 0;
}
