#ifndef LSMKV_SRC_KEYCACHE_H
#define LSMKV_SRC_KEYCACHE_H
#include <list>
#include <queue>
#include "memtable.h"
#include "table.h"
#include "../utils.h"
#include "version.h"
#include <map>
#include <ranges>
#include <algorithm>
#include <functional>
namespace LSMKV {
	class KeyCache {
		typedef Table::TableIterator TableIterator;
	public:
		KeyCache(const KeyCache&) = delete;
		const KeyCache& operator=(const KeyCache&) = delete;
		explicit KeyCache(const std::string& db_path, Version* v) {
			std::vector<std::string> dirs;
			std::vector<std::string> files;
			utils::scanDirs(db_path, dirs);
			Slice result;
			char* raw = new char[16 * 1024];
			Option op;
			RandomReadableFile* file;
			Status s;
			std::string path_name = db_path + "/";
			size_t dir_size;
			size_t file_size;
			uint64_t dir_level;

			for (auto& dir : dirs) {
				files.clear();
				dir_size = path_name.size();
				path_name.append(dir);
				path_name.append("/");
				file_size = path_name.size();
				utils::scanDir(path_name, files);
				dir_level = atoll(&dir[6]);
				for (auto& file_name : files) {
					path_name.append(file_name);
					v->LoadStatus(dir_level, stoll(file_name));
					s = NewRandomReadableFile(path_name, &file);
					if (!s.ok()) {
						continue;
					}
					s = file->Read(0, 16 * 1024, &result, raw);
					if (!s.ok()) {
						continue;
					}
					auto it = new TableIterator(new Table(result.data(), stoll(file_name), op));
					cache.emplace(it->timestamp(), it);
//					cache.emplace_back();
					delete file;
					path_name.resize(file_size);
				}
				path_name.resize(dir_size);
			}
			delete[] raw;
		};
		void ToNewLevel(std::vector<uint64_t>& file_nos) {

		}
		// -1 error
		// 0 ok
		// 1 JustMove
		// 2 dont know
		int CompactionSST(uint64_t level, uint64_t& file_no,
			std::vector<uint64_t> old_file_nos[2],
			std::vector<uint64_t>& new_file_nos,
			std::vector<Slice>& need_to_write,
			std::set<uint64_t>& this_level_files,
			std::set<uint64_t>& next_level_files) {
			// NEED LEVEL-N AND LEVEL-N+1 FILE_NOS
			// TODO CHANGE TO UNORDERED_MAP(?)
			// I CAN'T READ this ANY MORE !!!!!!
			// a piece of shit
			std::multimap<uint64_t, std::multimap<uint64_t, TableIterator*>::iterator> choose_this_level;
			auto choose_it = choose_this_level.begin();
			std::pair<uint64_t, TableIterator*> it;
			uint64_t size = this_level_files.size() - level == 0 ? 0 : (1 << (level + 1));
			TableIterator* tmp;
			uint64_t timestamp = 0;
			FindCompactionNextLevel(
				timestamp,
				this_level_files,
				size,
				[&](auto& rit) {
				  if ((choose_it = choose_this_level.find(it.first)) != choose_this_level.end()) {
					  it = *rit;
					  tmp = (choose_it->second)->second;
					  if (it.second->SmallestKey() < tmp->SmallestKey()
						  || ((it.second->SmallestKey() == tmp->SmallestKey())
							  && (it.second->LargestKey() < tmp->LargestKey()))) {
						  choose_it->second = rit;
					  }
				  } else {
					  choose_this_level.insert({ it.first, rit });
				  }
				});
			// DIRECTLY Move TO NEXT LEVEL
			if (next_level_files.empty() && level != 0) {
				for (auto& cit : choose_this_level) {
					cit.second->second->setTimestamp(timestamp);
					new_file_nos.emplace_back(cit.second->second->file_no());
				}
				return 2;
			}
			uint64_t smallest_key = UINT64_MAX;
			uint64_t largest_key = 0;
			for (auto& cit : choose_this_level) {
				tmp = cit.second->second;
//				cache.erase(cit.second);
				timestamp = std::max(timestamp, tmp->timestamp());
				smallest_key = std::min(smallest_key, tmp->SmallestKey());
				largest_key = std::max(largest_key, tmp->LargestKey());
//				old_file_nos.insert({ level, tmp->file_no() });
			}

			// Read ALL Conflicted file In NextLevel
			FindCompactionNextLevel(
				timestamp,
				next_level_files,
				UINT64_MAX,
				[&](auto& rit) {
				  it = *rit;
				  if (it.second->InRange(smallest_key, largest_key)) {
					  choose_this_level.insert({ (*rit).first, rit });
				  }
				});
			// a vector has tableiterator stauts
			// 0 not read
			// 1 end
			// -1 error
			std::vector<std::pair<int, TableIterator*>> need_to_merge;
			for (auto& cit : choose_this_level) {
				tmp = cit.second->second;
				tmp->seekToFirst();
				need_to_merge.emplace_back(0, tmp);
			}
			Merge(need_to_merge, need_to_write);

		}

		template<typename function>
		void FindCompactionNextLevel(uint64_t& timestamp, std::set<uint64_t>& this_level_files,
			uint64_t size,
			function const& callback) {
			std::pair<uint64_t, TableIterator*> it;
			for (auto rit = cache.begin(); rit != cache.end(); rit++) {
				it = *rit;
				if (size > 0) {
					if (this_level_files.count(it.second->file_no())) {
						timestamp = std::max(it.first, timestamp);
						callback(rit);
					}
				}
			}
		}

		void Merge(std::vector<std::pair<int, TableIterator*>>& need_to_merge, std::vector<Slice>& need_to_write) {

		}

		void reset() {
			if (tmp_cache != nullptr) {
				delete tmp_cache;
			}
			for (auto& it : cache) {
				delete it.second;
			}
			cache.clear();
		}
		char* ReserveCache(size_t size, const uint64_t& file_no) {
			assert(tmp_cache == nullptr);
			tmp_cache = new Table(file_no);
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

		void PushCache(const char* tmp, const Option& op) {
			assert(tmp_cache != nullptr);
			tmp_cache->pushCache(tmp, op);
			auto it = new TableIterator(tmp_cache);
			cache.emplace(it->timestamp(), it);
			tmp_cache = nullptr;
		}

		std::string get(const uint64_t& key) {
			TableIterator* table;
			for (auto& it : std::ranges::reverse_view(cache)) {
				table = it.second;
				table->seek(key);
				if (table->hasNext()) {
					return { table->value().data(), table->value().size() };
				}
			}
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
			TableIterator* table;
			for (auto& it : std::ranges::reverse_view(cache)) {
				table = it.second;
				table->seek(key);
				if (table->hasNext()) {
					offset = DecodeFixed64(table->value().data());
					return true;
				}
			}
			return false;
		}

	private:
		// key is timestamp
		// TODO change key to level,because in level its ordered
		std::multimap<uint64_t, TableIterator*> cache;
		Table* tmp_cache = nullptr;
// 		struct cmp {
//			// TODO  PASS BY VALUE MAY BE LITTLE FASTER
//			bool operator()(const key_pair& a, const key_pair& b) const {
//				// FILE_NO MAKE NO SENSE IN MAP ORDER
//				return a.second < b.second;
//			}
//		};
// lowwer 8 bytes is timestamp, upper is file_no
//		std::map<key_pair, TableIterator*, cmp> cache;
//		std::priority_queue<Table*, std::vector<Table*>, cmp> cache;

//		std::vector<Iterator*> cache;
	};
}

#endif //LSMKV_SRC_KEYCACHE_H
