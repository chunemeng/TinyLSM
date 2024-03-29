#ifndef LSMKV_SRC_TABLE_H
#define LSMKV_SRC_TABLE_H
#include <string>
#include "memtable.h"
#include "../utils/option.h"
#include "../utils/coding.h"
#include "../utils/bloomfilter.h"
namespace LSMKV {

	//instance of sst
	static constexpr uint64_t bloom_size = 8192;
	class Table {
	public:
		explicit Table() {
		}
		char* reserve(size_t size) {
			return arena.allocate(size);
		}
		explicit Table(const char* tmp, Option option) {
			if ((isFilter = option.isFilter)) {
				char* bloomer = arena.allocate(bloom_size);
				strncpy(bloomer, tmp + 32, 8192);
				bloom = Slice(bloomer, 8192);
			}
			timestamp = DecodeFixed64(tmp);
			uint64_t size = DecodeFixed64(tmp + 8);
			uint64_t offset = bloom_size + 32;

			// TODO MAY NEED COMPARE!!
			Slice value_offset;
			sst.reserve(size * 20);
			char* buf = arena.allocate(size * 20);
			memcpy(buf, tmp + 8224, size * 20);
			for (uint64_t i = 0; i < size * 20; i += 20) {
				value_offset = Slice(tmp + offset + 8 + i, 12);
				sst.emplace_back(DecodeFixed64(tmp + offset + i), value_offset);

			}
		}
		void pushCache(const char* tmp) {
			bloom = Slice(tmp + 32, 8192);
			timestamp = DecodeFixed64(tmp);
			uint64_t size = DecodeFixed64(tmp + 8);
			uint64_t offset = bloom_size + 32;

			// TODO MAY NEED COMPARE!!
			sst.reserve(size * 20);
			for (uint64_t i = 0; i < size * 20; i += 20) {
				// NO NEED TO ALLOCATE NEW MEMORY
				sst.emplace_back(DecodeFixed64(tmp + offset + i), std::move(Slice(tmp + offset + 8 + i, 12)));
			}
		}
		uint64_t GetTimestamp() const {
			return timestamp;
		}

		bool KeyMatch(const uint64_t& key) {
			return KeyMayMatch(key, bloom);
		}

		~Table() {
			if (isFilter) {
				delete[] bloom.data();
			}
		}

		bool operator<(const Table& rhs) const {
			return timestamp < rhs.timestamp;
		}

		bool operator<(const uint64_t& rhs) const {
			return timestamp < rhs;
		}

		std::string get(const uint64_t& key) {
			if (!KeyMatch(key)) {
				return "";
			}
			return BinarySearchGet(key);
		}

	private:

		std::string BinarySearchGet(const uint64_t& key) const {
			// TODO 斐波那契分割优化(?)
			uint64_t low = 1, high = sst.size() - 1, mid;
			if (key == sst[0].first) {
				return { sst[0].second.data(), 12 };
			}
			while (low <= high) {
				mid = low + ((high - low) >> 1);
				if (key < sst[mid].first)
					high = mid - 1;
				else if (key > sst[mid].first)
					low = mid + 1;
				else
					return { sst[mid].second.data(), 12 };
			}
			return "";
		}
		bool isFilter = true;
		Arena arena;
		std::vector<std::pair<uint64_t, Slice>> sst;
		Slice bloom;
		uint64_t timestamp = 0;
	};
}

#endif //LSMKV_SRC_TABLE_H
