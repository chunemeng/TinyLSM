
#include "table.h"
namespace LSMKV {


	uint64_t Table::BinarySearchGet(const uint64_t& start,const uint64_t& end, const uint64_t& key) const {
		// TODO 斐波那契分割优化(?)
		// NOT INCLUDE END
		assert(start < end && start < sst.size() && end < sst.size());
		uint64_t low = start + 1, high = end - 1, mid;
		if (key == sst[start].first) {
			return start;
		}
		while (low <= high) {
			mid = low + ((high - low) >> 1);
			if (key < sst[mid].first)
				high = mid - 1;
			else if (key > sst[mid].first)
				low = mid + 1;
			else
				return mid;
		}
		return sst.size();
	}
}