#include <cstdint>
#ifndef FILEMETA_H
#define MISC_XML_UTILS_FILEMETA_H

struct FileMeta {
	// 8bytes timestamp 8 bytes kv_size
	// 8bytes largest 	8 bytes smallest
	uint64_t timestamp;
	uint64_t size;
	const char* largest;
	const char* smallest;
};

#endif //FILEMETA_H
