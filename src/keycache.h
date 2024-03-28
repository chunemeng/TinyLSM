#ifndef LSMKV_SRC_KEYCACHE_H
#define LSMKV_SRC_KEYCACHE_H
#include <list>
#include "memtable.h"
#include "table.h"

namespace LSMKV {
	class KeyCache {
	public:
		KeyCache(){};
		char* ReserveCache(size_t size) {
			assert(tmp_cache == nullptr);
			tmp_cache = new Table();
			return tmp_cache->reserve(size);
		}


		void PushCache(const char* tmp) {
			assert(tmp_cache != nullptr);
			tmp_cache->pushCache(tmp);
			cache.emplace_back(tmp_cache);
			tmp_cache = nullptr;
		}
		~KeyCache(){
			for (auto &it : cache) {
				delete it;
			}
		}
	private:
		Table* tmp_cache = nullptr;
		std::list<Table*> cache;
	};
}

#endif //LSMKV_SRC_KEYCACHE_H
