#ifndef LSMKV_SRC_KEYCACHE_H
#define LSMKV_SRC_KEYCACHE_H
#include <list>
#include <queue>
#include "memtable.h"
#include "table.h"
#include "../utils.h"
#include <map>
#include <scoped_allocator>

namespace LSMKV {
	class KeyCache {
		typedef Table::TableIterator Iterator;
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
					s = file->ReadAll(&result, raw);
					if (!s.ok()) {
						continue;
					}
					cache.emplace_back(new Iterator(new Table(result.data(), op)));
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
		void scan(const uint64_t& K1, const uint64_t& K2, std::list<std::pair<uint64_t, std::string>>& list) {
			std::map<key_type, std::pair<uint64_t, Slice>> key_map;
			auto it = key_map.begin();
			for (auto& table : cache) {
				table->seek(K1, K2);
				while (table->hasNext()) {
					if ((it = key_map.find(table->key())) == key_map.end()) {
						key_map.insert(std::make_pair(table->key(),
							std::make_pair(table->timestamp(), table->value())));
					} else {
						if (it->second.first < table->timestamp()) {
							it->second = std::make_pair(table->timestamp(), table->value());
						}
					}
				}
			}
			for (auto& iter : key_map) {
				list.emplace_back(iter.first, std::string{ iter.second.second.data(), iter.second.second.size() });
			}
		}

		void PushCache(const char* tmp) {
			assert(tmp_cache != nullptr);
			tmp_cache->pushCache(tmp);
			cache.emplace_back(new Iterator(tmp_cache));
			tmp_cache = nullptr;
		}
		std::string get(const uint64_t& key) {
			uint64_t current = UINT64_MAX;
			std::string result;
			for (auto& table : cache) {
				if (table->timestamp() < current) {
					table->seek(key);
					if (table->hasNext()) {
						result = { table->value().data(), table->value().size() };
					}
				}
			}
			return { result.empty() ? "" : result };
			//	if (table->GetTimestamp() < current) {
			//		if (!((result = table->get(key)).empty())) {
			//			current = table->GetTimestamp();
			//		}
			//	}
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
		// TODO TO SKIPLIST(?)
//		std::priority_queue<Table*, std::vector<Table*>, cmp> cache;
		std::vector<Iterator*> cache;
	};
}

#endif //LSMKV_SRC_KEYCACHE_H
