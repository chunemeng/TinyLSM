#ifndef LSMKV_SRC_KEYCACHE_H
#define LSMKV_SRC_KEYCACHE_H
#include <list>
#include <queue>
#include "memtable.h"
#include "table.h"
#include "../utils.h"
#include <map>
#include <ranges>


namespace LSMKV {
	class KeyCache {
		typedef Table::TableIterator TableIterator;
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
					auto it = new TableIterator(new Table(result.data(), op));
					cache.insert(std::make_pair(it->timestamp(), it));
//					cache.emplace_back();
					delete file;
				}
			}
			delete[] raw;
		};
		void reset() {
			if (tmp_cache != nullptr) {
				delete tmp_cache;
			}
			for (auto& it : cache) {
				delete it.second;
			}
			cache.clear();
		}
		char* ReserveCache(size_t size) {
			assert(tmp_cache == nullptr);
			tmp_cache = new Table();
			return tmp_cache->reserve(size);
		}
		void scan(const uint64_t& K1, const uint64_t& K2, std::map<key_type, std::string>& key_map) {
			auto it = key_map.begin();
			TableIterator* table;
			for (auto& table_pair : cache) {
				table = table_pair.second;
				table->seek(K1, K2);
				while (table->hasNext()) {
					key_map.insert(std::make_pair(table->key(),
						std::string{ table->value().data(), table->value().size() }));
					table->next();
				}
			}
		}

		void PushCache(const char* tmp) {
			assert(tmp_cache != nullptr);
			tmp_cache->pushCache(tmp);
			auto it = new TableIterator(tmp_cache);
			cache.insert(std::make_pair(it->timestamp(), it));
			tmp_cache = nullptr;
		}
		std::string get(const uint64_t& key) {
			TableIterator *table;
			for (auto it= cache.rbegin(); it != cache.rend(); it++) {
				table = it->second;
				table->seek(key);
				if (table->hasNext()) {
					return { table->value().data(), table->value().size() };
				}
			}
//			for (auto & it : std::ranges::reverse_view(cache)) {
//				table = it.second;
//				table->seek(key);
//				if (table->hasNext()) {
//					return { table->value().data(), table->value().size() };
//				}
//			}
			return {};
		}

		~KeyCache() {
			if (tmp_cache != nullptr) {
				delete tmp_cache;
			}
			for (auto& it : cache) {
				delete it.second;
			}
			cache.clear();
		}
		bool empty() const {
			return cache.empty();
		}
		bool GetOffset(const uint64_t& key, uint64_t& offset) {
			TableIterator *table;
			for (auto it= cache.rbegin(); it != cache.rend(); it++) {
				table = it->second;
				table->seek(key);
				if (table->hasNext()) {
					offset = DecodeFixed64(table->value().data());
					return true;
				}
			}
//			for (auto & it : std::ranges::reverse_view(cache)) {
//				table = it.second;
//				table->seek(key);
//				if (table->hasNext()) {
//					offset = DecodeFixed64(table->value().data());
//					return true;
//				}
//			}
			return false;
		}
	private:
		Table* tmp_cache = nullptr;
//		std::priority_queue<Table*, std::vector<Table*>, cmp> cache;
		std::multimap<uint64_t, TableIterator*> cache;
//		std::vector<Iterator*> cache;
	};
}

#endif //LSMKV_SRC_KEYCACHE_H
