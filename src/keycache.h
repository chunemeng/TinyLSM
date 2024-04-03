#ifndef LSMKV_SRC_KEYCACHE_H
#define LSMKV_SRC_KEYCACHE_H

#include <list>
#include <queue>
#include "memtable.h"
#include "table.h"
#include "../utils.h"
#include "version.h"
#include <map>
#include <set>
#include <unordered_set>
#include <ranges>
#include <queue>

namespace LSMKV {
    class KeyCache {
        typedef Table::TableIterator TableIterator;
    public:
        KeyCache(const KeyCache &) = delete;

        const KeyCache &operator=(const KeyCache &) = delete;

        explicit KeyCache(const std::string &db_path, Version *v) {
            for (int i = 0; i < 8; ++i) {
                cache.emplace(i, std::multimap<uint64_t, TableIterator *>{});
            }
            std::vector<std::string> dirs;
            std::vector<std::string> files;
            utils::scanDirs(db_path, dirs);
            Slice result;
            char *raw = new char[16 * 1024];
            Option op;
            RandomReadableFile *file;
            Status s;
            std::string path_name = db_path + "/";
            size_t dir_size;
            size_t file_size;
            uint64_t dir_level;

            for (auto &dir: dirs) {
                files.clear();
                dir_size = path_name.size();
                path_name.append(dir);
                path_name.append("/");
                file_size = path_name.size();
                utils::scanDir(path_name, files);
                dir_level = atoll(&dir[6]);
                for (auto &file_name: files) {
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
                    cache[dir_level].emplace(it->timestamp(), it);
//					cache.emplace_back();
                    delete file;
                    path_name.resize(file_size);
                }
                path_name.resize(dir_size);
            }
            delete[] raw;
        };


        // return timestamp to update the version
        uint64_t CompactionSST(uint64_t level, uint64_t file_no,uint64_t size,
                               std::vector<uint64_t> old_file_nos[2],
                               std::vector<uint64_t> &need_to_move,
                               std::vector<Slice> &need_to_write
                               ) {
            // NEED LEVEL-N AND LEVEL-N+1 FILE_NOS
            // TODO CHANGE TO UNORDERED_MAP(?)

            // key is timestamp, value is iterator
            std::multimap<uint64_t, std::multimap<uint64_t, TableIterator *>::iterator> choose_this_level;
            // tmp value
            auto choose_it = choose_this_level.begin();
            std::pair<uint64_t, TableIterator *> it;
            // size of sst need to read
            TableIterator *tmp;
            // timestamp need to update
            uint64_t timestamp = 0;

            std::set<uint64_t> file_location;
            // STRANGE BUG THAT CANT INSERT INTO UNORDERED_SET
            std::set<TableIterator *> wait_to_merge;
            // all byte size to allocate
            if (level > cache.size()) {
                size_t j = cache.size();
                for (size_t i = j; i <= level ; ++i) {
                    cache.emplace(i, std::multimap<uint64_t, TableIterator *>{});
                }
            }

            // store the sst with same timestamp
            std::priority_queue<std::multimap<uint64_t, TableIterator *>::iterator, std::vector<std::multimap<uint64_t, TableIterator *>::iterator>, cmp> que;
            bool isFull = false;
            // iterator comparator
            cmp cmp;
            // pick sst in size
            FindCompactionNextLevel(
                    level,
                    [&](auto &rit) {
                        it = *rit;
                        if (isFull) {
                            if (timestamp < it.first) {
                                while(!que.empty()) {
                                    choose_this_level.emplace(timestamp, que.top());
                                    que.pop();
                                }
                                return true;
                            } else {
                                if (cmp(rit, que.top())) {
                                    que.pop();
                                    que.push(rit);
                                }
                            }
                        } else {
                            if (size - 1 == choose_this_level.size() + que.size()) {
                                isFull = true;
                            }
                            if (it.first == timestamp) {
                                que.push(rit);
                            } else {
                                while (!que.empty()) {
                                    choose_this_level.emplace(timestamp, que.top());
                                    que.pop();
                                }
                                timestamp = it.first;
                                que.push(rit);
                            }
                        }
                        return false;
                    }

            );
            while(!que.empty()) {
                choose_this_level.emplace(timestamp, que.top());
                que.pop();
            }

            // DIRECTLY Move TO NEXT LEVEL
            if (cache[level + 1].empty() && level != 0) {
                for (auto &cit: choose_this_level) {
                    tmp = cit.second->second;
                    tmp->setTimestamp(timestamp);
                    need_to_move.emplace_back(tmp->file_no());
                    cache[level].erase(cit.second);
                    cache[level + 1].emplace(timestamp, tmp);
                }
                return timestamp;
            }
            // read smallest and largest key in this level
            uint64_t smallest_key = UINT64_MAX;
            uint64_t largest_key = 0;
            for (auto &cit: choose_this_level) {
                tmp = (*(cit.second)).second;
                smallest_key = std::min(smallest_key, tmp->SmallestKey());
                largest_key = std::max(largest_key, tmp->LargestKey());
                file_location.emplace(tmp->file_no());
                tmp->seekToFirst();
                auto t = wait_to_merge.emplace(tmp);
                cache[level].erase(cit.second);
            }
            choose_this_level.clear();

            // Read ALL Conflicted file In NextLevel
            FindCompactionNextLevel(
                    level + 1,
                    [&](auto &rit) {
                        it = *rit;
                        if (it.second->InRange(smallest_key, largest_key)) {
                            timestamp = std::max(it.first, timestamp);
                            choose_this_level.emplace((*rit).first, rit);
                        }
                        return false;
                    });

            // START ALL ITERATOR AND START STATE
            for (auto &cit: choose_this_level) {
                tmp = (*(cit.second)).second;
                tmp->seekToFirst();
                auto t = wait_to_merge.emplace(tmp);
                cache[level + 1].erase(cit.second);
            }

            // START MERGE
            Merge(file_no, level, timestamp, wait_to_merge, need_to_move, need_to_write, file_location,
                  old_file_nos);
            return timestamp;
        }

        template<typename function>
        void FindCompactionNextLevel(uint64_t level,
                                     function const &callback) {
            std::pair<uint64_t, TableIterator *> it;
            for (auto rit = cache[level].begin(); rit != cache[level].end(); rit++) {
                if (callback(rit)) {
                    break;
                }
            }
        }

        void Merge(uint64_t file_no,
                   uint64_t level,
                   uint64_t timestamp,
                   std::set<TableIterator *> &wait_to_merge,
                   std::vector<uint64_t> &need_to_move,
                   std::vector<Slice> &need_to_write,
                   std::set<uint64_t> &file_location,
                   std::vector<uint64_t> old_file_nos[2]) {
            // ERROR AFTER MERGE THE SIZE MAY BE NOT SAME TO SIZE
            // file_no to generate new file
            // if the last one is full and not compaction will directly move
            // THE OFFSET START TO WRITE (BLOOM_SIZE AND HEADER_SIZE)
            uint64_t key_offset = 8224;
            // KEY_CACHE OPTION

            Option op;
            // TableIterator need to move next
            std::vector<decltype(wait_to_merge.begin())> need_to_next;
            while (!wait_to_merge.empty()) {
                table_cache = new Table(file_no++);
                // TODO FIND A GOOD WAY TO RESERVE
                char *tmp = table_cache->reserve(16384);
                Slice tmp_slice;
                uint64_t wait_insert_key = UINT64_MAX;
                uint64_t key_timestamp;
                Slice value;
                // CURRENT TABLE_CACHE.SIZE
                uint64_t t_size = 0;
                while (t_size < 408 && !wait_to_merge.empty()) {
                    wait_insert_key = UINT64_MAX;
                    key_timestamp = 0;
                    for (auto it = wait_to_merge.begin(); it != wait_to_merge.end(); it++) {
                        auto rit = *it;
                        // IF THE KEY IS SMALLER THAN STORED IN WAIT_INSERT_KEY
                        // UPDATE TO THIS ONE
                        if (wait_insert_key > rit->key()) {
                            need_to_next.clear();
                            need_to_next.emplace_back(it);
                            key_timestamp = rit->timestamp();
                            value = rit->value();
                            wait_insert_key = rit->key();
                            continue;
                        }
                        if (wait_insert_key == rit->key()) {
                            need_to_next.emplace_back(it);
                            // UPDATE VALUE IF TIMESTAMP IS NEW
                            // second is for key == uint64_max
                            if (key_timestamp < rit->timestamp()) {
                                key_timestamp = rit->timestamp();
                                value = rit->value();
                            }
                        }
                    }
                    ++t_size;
                    // WRITE KEY AND OFFSET
                    EncodeFixed64(tmp + key_offset, wait_insert_key);
                    memcpy(tmp + key_offset + 8, value.data(), value.size());
                    key_offset = key_offset + 20;
                    for (auto &index: need_to_next) {
                        (*index)->next();
                        if (!((*index)->hasNext())) {
                            auto it = (*index)->file_no();
                            // find the level of the file
                            if (file_location.count(it)) {
                                old_file_nos[0].emplace_back(it);
                            } else {
                                old_file_nos[1].emplace_back(it);
                            }
                            delete (*index);
                            // if erase first, the iterator will invalid
                            wait_to_merge.erase(index);
                        }
                    }
                    need_to_next.clear();
                }
                need_to_next.clear();
                //WRITE HEADER
                // WRITE TIMESTAMP
                EncodeFixed64(tmp, timestamp);
                // WIRTE VALUE SIZE
                EncodeFixed64(tmp + 8, t_size);
                // WRITE MIN KEY
                memcpy(tmp + 16, tmp + 8224, 8);
                // WRITE MAX KEY
                EncodeFixed64(tmp + 24, wait_insert_key);
                tmp_slice = Slice(tmp, t_size * 20 + 8224);
                // todo bug when use emplace_back
                need_to_write.emplace_back(tmp_slice);
                auto iterator = new TableIterator(table_cache);
                iterator->setTimestamp(timestamp);
                table_cache->pushCache(tmp_slice.data(), op);
                cache[level + 1].emplace(timestamp, iterator);
                key_offset = 8224;
            }
            table_cache = nullptr;
        }

        void reset() {
            delete table_cache;
            for (auto &level: cache) {
                for (auto &it: level.second) {
                    delete it.second;
                }
                level.second.clear();
            }
        }

        char *ReserveCache(size_t size, const uint64_t &file_no) {
            assert(table_cache == nullptr);
            table_cache = new Table(file_no);
            return table_cache->reserve(size);
        }

        void scan(const uint64_t &K1, const uint64_t &K2, std::map<key_type, std::string> &key_map) {
            auto it = key_map.begin();
            TableIterator *table;
            for (auto &level: cache) {
                for (auto &table_pair: level.second) {
                    table = table_pair.second;
                    table->seek(K1, K2);
                    while (table->hasNext()) {
                        key_map.insert(std::make_pair(table->key(),
                                                      std::string{table->value().data(), table->value().size()}));
                        table->next();
                    }
                }
            }
        }

        void PushCache(const char *tmp, const Option &op) {
            assert(table_cache != nullptr);
            table_cache->pushCache(tmp, op);
            auto it = new TableIterator(table_cache);
            cache[0].emplace(it->timestamp(), it);
            table_cache = nullptr;
        }

        std::string get(const uint64_t &key) {
            TableIterator *table;
            for (auto &level: cache) {
                for (auto &it: std::ranges::reverse_view(level.second)) {
                    table = it.second;
                    table->seek(key);
                    if (table->hasNext()) {
                        return {table->value().data(), table->value().size()};
                    }
                }
            }
            return {};
        }

        ~KeyCache() {
            if (table_cache != nullptr) {
                delete table_cache;
            }
            for (auto &level: cache) {
                for (auto &it: level.second) {
                    delete it.second;
                }
            }
            cache.clear();
        }

        bool empty() const {
            return cache.empty();
        }

        bool GetOffset(const uint64_t &key, uint64_t &offset) {
            TableIterator *table;
            for (auto &level: cache) {
                for (auto &it: std::ranges::reverse_view(level.second)) {
                    table = it.second;
                    table->seek(key);
                    if (table->hasNext()) {
                        offset = DecodeFixed64(table->value().data());
                        return true;
                    }
                }
            }
            return false;
        }

    private:
        // key is timestamp
        // No need to level_order, because it also needs to keep key in order
        // otherwise it sames to this one
        // key is level , value's key is timestamp

        std::map<uint64_t, std::multimap<uint64_t, TableIterator *>> cache;
        Table *table_cache = nullptr;

        // the comparator of TableIterator*
        struct cmp {
            bool operator()(const std::multimap<uint64_t, TableIterator *>::iterator &left,
                            const std::multimap<uint64_t, TableIterator *>::iterator &right) {
                return left->second->SmallestKey() < right->second->SmallestKey();
            }

        };
    };
}

#endif //LSMKV_SRC_KEYCACHE_H
