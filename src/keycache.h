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
			char* raw = new char[16 * 1024];
			Option op;
			SequentialFile* file;
			Status s;
			std::string path_name;
			for (auto& dir : dirs) {
				files.clear();
				path_name = db_path + "/" + dir;
				utils::scanDir(path_name, files);
				for (auto& file_name : files) {
					s = NewSequentialFile(path_name + "/" + file_name, &file);
					if (!s.ok()) {
						continue;
					}
					file->ReadAll(&result, raw);
					cache.emplace_back(new Table(result.data(), op));
					delete file;
				}
			}
			delete[] raw;
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
			for (auto& table : cache) {
				if (table->GetTimestamp() < current) {
					if (!((result = table->get(key)).empty())) {
						current = table->GetTimestamp();
					}
				}
			}
			return { result.empty() ? "" : result };
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
