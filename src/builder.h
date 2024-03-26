#ifndef BUILDER_H
#define BUILDER_H

#include <string>
#include "status.h"
#include "../utils/filemeta.h"
#include "version.h"
namespace lsm {
	class Iterator;

	static inline Status BuildTable(const std::string& dbname, Version* v, Iterator* iter);
}

#endif //BUILDER_H
