#ifndef LSMKV_SRC_KEYCACHE_H
#define LSMKV_SRC_KEYCACHE_H
#include <list>
#include "memtable.h"
#include "table.h"
#include "../utils.h"

namespace LSMKV {
	class KeyCache {
	public:
		KeyCache(const KeyCache&) = delete;
		const KeyCache& operator=(const KeyCache&) = delete;
		explicit KeyCache(const std::string& db_path) {
			std::vector<std::string> dirs;
			std::vector<std::string> files;
			utils::scanDirs(db_path, dirs);
			Slice result;
			std::string raw;
			Option op;
			SequentialFile* file;
			for (auto& dir : dirs) {
				files.clear();
				utils::scanDir(dir, files);
				for (auto& file_name : files) {
					NewSequentialFile(db_path + "/"+dir +"/"+ file_name, &file);
					file->ReadAll(&result,raw);
					cache.emplace_back(new Table(result.data(),op));
					delete file;
					raw.clear();
				}
			}
		};
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
		std::string get(uint64_t key) {
			uint64_t current = UINT64_MAX;
			std::string result;
			for (auto & table:cache) {
				if (table->GetTimestamp() < current) {
					if (!((result = table->get(key)).empty())) {
						current = table->GetTimestamp();
					}
				}
			}
			return {result.empty() ? "" : result};
		}

		~KeyCache() {
			if (tmp_cache != nullptr) {
				delete tmp_cache;
			}
			for (auto& it : cache) {
				delete it;
			}
		}
	private:
		Table* tmp_cache = nullptr;
		std::list<Table*> cache;
	};
}

#endif //LSMKV_SRC_KEYCACHE_H
