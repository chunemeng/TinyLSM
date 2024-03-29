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
			sst = new MemTable();
		}
		char* reserve(size_t size) {
			return sst->reserve(size);
		}
		explicit Table(const char* tmp, Option option) {
			if ((isFilter = option.isFilter)) {
				char* bloomer = new char[8192];
				strncpy(bloomer, tmp + 32, 8192);
				bloom = Slice(bloomer, 8192);
			}
			sst = new MemTable();
			timestamp = DecodeFixed64(tmp);
			uint64_t size = DecodeFixed64(tmp + 8);
			uint64_t offset;

			// TODO MAY NEED COMPARE!!
			Slice value_offset;
			for (uint64_t i = size * 20; i >= 20; i -= 20) {
				offset = i + bloom_size - 20;
				value_offset = Slice(tmp + offset + 8, 12);
				sst->put(DecodeFixed64(tmp + offset)
					, value_offset);
			}
		}
		void pushCache(const char* tmp) {
			assert(sst != nullptr);
			bloom = Slice(tmp + 32, 8192);
			timestamp = DecodeFixed64(tmp);
			uint64_t size = DecodeFixed64(tmp + 8);
			uint64_t offset;

			// TODO MAY NEED COMPARE!!
			for (uint64_t i = size * 20; i >= 20; i -= 20) {
				offset = i + bloom_size - 20;
				// NO NEED TO ALLOCATE NEW MEMORY
				sst->put(DecodeFixed64(tmp + offset)
					, std::move(Slice(tmp + offset + 8, 12)));
			}
		}
		uint64_t GetTimestamp() const{
			return timestamp;
		}

		bool KeyMatch(const uint64_t& key) {
			return KeyMayMatch(key, bloom);
		}

		~Table() {
			if (isFilter) {
				delete sst;
				delete[] bloom.data();
			}
		}

		bool operator<(const Table& rhs) const {
			return timestamp < rhs.timestamp;
		}

		bool operator<(const uint64_t& rhs) const {
			return timestamp < rhs;
		}

		std::string get(uint64_t key) {
			if (!KeyMatch(key)) {
				return "";
			}
			return sst->get(key);
		}
	private:
		bool isFilter = true;
		MemTable* sst;
		Slice bloom;
		uint64_t timestamp = 0;
	};
}

#endif //LSMKV_SRC_TABLE_H
