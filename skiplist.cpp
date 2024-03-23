#include "skiplist.h"
#include <optional>

namespace skiplist {


	skiplist_type::skiplist_type(double p) : table(p, &a){
	}
//	void skiplist_type::put(key_type key, const value_type &val) {
//		table.insert(key, val);
//	}
	void skiplist_type::put(key_type key, value_type &&val) {
		table.insert(key, std::move(val));
	}
	std::string skiplist_type::get(key_type key) const {
		return table.find(key);
	}
	int skiplist_type::query_distance(key_type key) const {
		return table.query(key);
	}

} // namespace skiplist
