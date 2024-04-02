#ifndef BUILDER_H
#define BUILDER_H

#include <string>
#include "status.h"
#include "../utils/filemeta.h"
#include "version.h"
#include "keycache.h"
namespace LSMKV {
	class Iterator;

	Status BuildTable(const std::string& dbname, Version* v, Iterator* iter, FileMeta& file,KeyCache* kc);
	Status SSTCompaction(uint64_t level,uint64_t file_no, Version *v, KeyCache *kc);
	Status MoveToNewLevel(uint64_t level,const uint64_t& timestamp, std::vector<uint64_t>& new_files, Version* v);
	Status WriteSlice(std::vector<Slice>& need_to_write,uint64_t level,Version* v);
	uint64_t FindLevels(const std::string& dbname, Version* v);
}

#endif //BUILDER_H
