#ifndef OPTION_H
#define OPTION_H
#include "bloomfilter.h"

struct Option {
	bool create_if_missing = false;
	bool wal = false;
	const BloomFilter* bloom_filter = nullptr;
};

#endif //OPTION_H
