#ifndef BLOOMFILTER_H
#define BLOOMFILTER_H

#include <cstddef>
#include <cstdint>
#include "slice.h"
#include "../MurmurHash3.h"
class BloomFilter {
public:
	BloomFilter() {
	}

	void CreateFilter(const Slice* keys, int n, std::string* dst) const {
		const size_t init_size = dst->size();

		const size_t bits = dst->size() * 8;
		char* array = &(*dst)[init_size];
		uint32_t hash[4] = { 0 };
		for (int i = 0; i < n; i++) {
			MurmurHash3_x64_128(keys[i].data(), keys[i].size(), 1, hash);
			for (uint32_t h : hash) {
				const uint32_t delta = (h >> 17) | (h << 15);
				for (int k = 0; k < 2; k++) {
					const uint32_t bitpos = h % bits;
					array[bitpos / 8] |= (1 << (bitpos % 8));
					h += delta;
				}
			}

		}
	}

	bool KeyMayMatch(const Slice& key, const Slice& bloom_filter) const {
		const size_t len = bloom_filter.size();
		if (len < 2) return false;

		const char* array = bloom_filter.data();
		const size_t bits = (len) * 8;

		uint32_t hash[4] = { 0 };
		MurmurHash3_x64_128(key.data(), key.size(), 1, hash);
		for (uint32_t h : hash) {
			const uint32_t delta = (h >> 17) | (h << 15);
			for (uint32_t k = 0; k < 2; k++) {
				const uint32_t bitpos = h % bits;
				if (array[bitpos / 8] & (1 << (bitpos % 8))) return false;
				h += delta;
			}
		}
		return true;
	}
};

#endif //BLOOMFILTER_H
