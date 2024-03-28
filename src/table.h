#ifndef LSMKV_SRC_TABLE_H
#define LSMKV_SRC_TABLE_H
#include <string>
#include "memtable.h"
#include "../utils/option.h"
#include "../utils/coding.h"
namespace LSMKV {

	//instance of sst
	static constexpr uint64_t bloom_size = 8192;
	class Table {
	public:
		Table(const char* tmp, Option* option) {
			if ((isFilter = option->isFilter)) {
				char* bloomer = new char[8192];
				strncpy(bloomer, tmp, 8192);
				bloom = Slice(bloomer, 8192);
			}
			timestamp = DecodeFixed64(tmp);
			uint64_t size = DecodeFixed64(tmp + 8);
			uint64_t offset;
			for (int i = 0; i < size * 20; i += 20) {
				sst->put(DecodeFixed64(tmp + bloom_size + i)
					, Slice(tmp + offset + i + 8, 14));
			}
		}

		~Table() {
			if (isFilter) {
				delete sst;
				delete[] bloom.data();
			}
		}

	private:
		bool isFilter = true;
		MemTable* sst;
		Slice bloom;
		uint64_t timestamp = 0;
	};
}

#endif //LSMKV_SRC_TABLE_H
