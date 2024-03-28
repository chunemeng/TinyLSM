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

	uint64_t FindLevels(const std::string& dbname, Version* v);
}

#endif //BUILDER_H
