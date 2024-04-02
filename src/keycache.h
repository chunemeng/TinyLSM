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

namespace LSMKV {
    class KeyCache {
        typedef Table::TableIterator TableIterator;
    public:
        KeyCache(const KeyCache &) = delete;

        const KeyCache &operator=(const KeyCache &) = delete;

        explicit KeyCache(const std::string &db_path, Version *v) {
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
                    cache.emplace(it->timestamp(), it);
//					cache.emplace_back();
                    delete file;
                    path_name.resize(file_size);
                }
                path_name.resize(dir_size);
            }
            delete[] raw;
        };

        void ToNewLevel(std::vector<uint64_t> &file_nos) {

        }

        // -1 error
        // 0 ok
        // 1 JustMove
        // 2 dont know
        uint64_t CompactionSST(uint64_t level, uint64_t file_no,
                               std::vector<uint64_t> old_file_nos[2],
                               std::vector<uint64_t> &need_to_move,
                               std::vector<Slice> &need_to_write,
                               std::set<uint64_t> &this_level_files,
                               std::set<uint64_t> &next_level_files) {
            // NEED LEVEL-N AND LEVEL-N+1 FILE_NOS
            // TODO CHANGE TO UNORDERED_MAP(?)

            // key is timestamp value is iterator
            std::multimap<uint64_t, std::multimap<uint64_t, TableIterator *>::iterator> choose_this_level;
            auto choose_it = choose_this_level.begin();
            std::pair<uint64_t, TableIterator *> it;
            uint64_t size = this_level_files.size() - ((level == 0) ? 0 : (1 << (level + 1)));
            TableIterator *tmp;
            uint64_t timestamp = 0;
            std::set<uint64_t> file_location;

            // pick sst in size
            FindCompactionNextLevel(
                    timestamp,
                    this_level_files,
                    size,
                    [&](auto &rit, uint64_t &size) {
                        it = *rit;
                        if ((choose_it = choose_this_level.find(it.first)) != choose_this_level.end()) {
                            tmp = (choose_it->second)->second;
                            if (it.second->SmallestKey() < tmp->SmallestKey()
                                || ((it.second->SmallestKey() == tmp->SmallestKey())
                                    && (it.second->LargestKey() < tmp->LargestKey()))) {
                                choose_it->second = rit;
                            }
                        } else {
                            choose_this_level.insert({it.first, rit});
                            size--;
                        }
                    });
            // DIRECTLY Move TO NEXT LEVEL
            if (next_level_files.empty() && level != 0) {
                for (auto &cit: choose_this_level) {
                    tmp = cit.second->second;
                    tmp->setTimestamp(timestamp);
                    need_to_move.emplace_back(tmp->file_no());
                    cache.erase(cit.second);
                    cache.emplace(timestamp, tmp);
                }
                return timestamp;
            }
            // read smallest and largest key in this level
            uint64_t smallest_key = UINT64_MAX;
            uint64_t largest_key = 0;
            for (auto &cit: choose_this_level) {
                tmp = cit.second->second;
                timestamp = std::max(timestamp, tmp->timestamp());
                smallest_key = std::min(smallest_key, tmp->SmallestKey());
                largest_key = std::max(largest_key, tmp->LargestKey());
                file_location.emplace(tmp->file_no());
            }

            // Read ALL Conflicted file In NextLevel
            FindCompactionNextLevel(
                    timestamp,
                    next_level_files,
                    cache.size(),
                    [&](auto &rit, uint64_t &size) {
                        it = *rit;
                        if (it.second->InRange(smallest_key, largest_key)) {
                            choose_this_level.insert({(*rit).first, rit});
                            --size;
                        }
                    });
            // STRANGE BUG THAT CANT INSERT INTO UNORDERED_SET
            std::set<TableIterator *> wait_to_merge;
            // all byte size to allocate
            uint64_t all_size = 0;
//            wait_to_merge.reserve(choose_this_level.size() + 1);
            // START ALL ITERATOR AND START STATE
            for (auto &cit: choose_this_level) {
                tmp = (*(cit.second)).second;
                tmp->seekToFirst();
                all_size += tmp->size();
                wait_to_merge.emplace(tmp);
                cache.erase(cit.second);
            }
            // START MERGE
            Merge(file_no, all_size, timestamp, wait_to_merge, need_to_move, need_to_write, file_location,
                  old_file_nos);
            return timestamp;
        }

        template<typename function>
        void FindCompactionNextLevel(uint64_t &timestamp, std::set<uint64_t> &this_level_files,
                                     uint64_t size,
                                     function const &callback) {
            std::pair<uint64_t, TableIterator *> it;
            for (auto rit = cache.begin(); rit != cache.end(); rit++) {
                it = *rit;
                if (size == 0 && timestamp < it.first) {
                    break;
                }
                if (this_level_files.count(it.second->file_no())) {
                    timestamp = std::max(it.first, timestamp);
                    callback(rit, size);
                }
            }
        }

        void Merge(uint64_t file_no,
                   uint64_t size,
                   uint64_t timestamp,
                   std::set<TableIterator *> &wait_to_merge,
                   std::vector<uint64_t> &need_to_move,
                   std::vector<Slice> &need_to_write,
                   std::set<uint64_t> &file_location,
                   std::vector<uint64_t> old_file_nos[2]) {
            // file_no to generate new file
            // if the last one is full and not compaction will directly move
            uint64_t key_offset = 8224;
            Option op;
            std::vector<decltype(wait_to_merge.begin())> need_to_next;
            while (size >= 408) {
                table_cache = new Table(file_no++);
                char *tmp = table_cache->reserve(16384);
                Slice tmp_slice = Slice(tmp, 16384);
                uint64_t wait_insert_key = UINT64_MAX;
                uint64_t key_timestamp = 0;
                Slice value;
                for (int j = 0; j < 408; ++j) {
                    wait_insert_key = UINT64_MAX;
                    for (auto it = wait_to_merge.begin(); it != wait_to_merge.end(); it++) {
                        auto rit = *it;
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
                            if (key_timestamp < rit->timestamp()) {
                                key_timestamp = rit->timestamp();
                                value = rit->value();
                            }
                        }
                    }
                    // WRITE KEY AND OFFSET
                    EncodeFixed64(tmp + key_offset, wait_insert_key);
                    memcpy(tmp + key_offset + 8, value.data(), value.size());
                    key_offset = key_offset + 20;
                    for (auto &index: need_to_next) {
                        (*index)->next();
                        if (!((*index)->hasNext())) {
                            auto it = (*index)->file_no();
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
                EncodeFixed64(tmp, timestamp);
                EncodeFixed64(tmp + 8, 408);
                memcpy(tmp + 16, tmp + 8224, 8);
                EncodeFixed64(tmp + 24, wait_insert_key);
                // todo bug when use emplace_back
                need_to_write.emplace_back(tmp_slice);
                auto iterator = new TableIterator(table_cache);
                iterator->setTimestamp(timestamp);
                table_cache->pushCache(tmp_slice.data(),op);
                cache.emplace(timestamp, iterator);
                size -= 408;
                key_offset = 8224;
            }
            // AFTER FIND LARGEST TIMESTAMP THEN UPDATE
            if (size > 0) {
                // Can Move Now
                auto begin = (*(wait_to_merge.begin()));
                if (wait_to_merge.size() == 1 && begin->AtStart()) {
                    begin->setTimestamp(timestamp);
                    need_to_move.emplace_back(begin->file_no());
                    cache.insert({begin->timestamp(), begin});
                } else {
                    table_cache = new Table(file_no++);
                    char *tmp = table_cache->reserve(8224 + size * 20);
                    Slice tmp_slice = Slice(tmp, 8224 + size * 20);
                    uint64_t wait_insert_key = UINT64_MAX;
                    uint64_t key_timestamp = 0;
                    Slice value;
                    for (int j = 0; j < size; ++j) {
                        wait_insert_key = UINT64_MAX;
                        for (auto it = wait_to_merge.begin(); it != wait_to_merge.end(); it++) {
                            auto rit = *it;
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
                                if (key_timestamp < rit->timestamp()) {
                                    key_timestamp = rit->timestamp();
                                    value = rit->value();
                                }
                            }
                        }
                        // WRITE KEY AND OFFSET
                        EncodeFixed64(tmp + key_offset, wait_insert_key);
                        memcpy(tmp + key_offset + 8, value.data(), value.size());
                        key_offset = key_offset + 20;
                        for (auto &index: need_to_next) {
                            (*index)->next();
                            if (!((*index)->hasNext())) {
                                auto it = (*index)->file_no();
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
                    EncodeFixed64(tmp, timestamp);
                    EncodeFixed64(tmp + 8, 408);
                    memcpy(tmp + 16, tmp + 8224, 8);
                    EncodeFixed64(tmp + 24, wait_insert_key);
                    // todo bug when use emplace_back
                    need_to_write.emplace_back(tmp_slice);
                    auto iterator = new TableIterator(table_cache);
                    iterator->setTimestamp(timestamp);
                    table_cache->pushCache(tmp_slice.data(),op);
                    cache.emplace(timestamp, iterator);
                }
            }
            table_cache = nullptr;
        }

        void reset() {
            delete table_cache;

            for (auto &it: cache) {
                delete it.second;
            }
            cache.clear();
        }

        char *ReserveCache(size_t size, const uint64_t &file_no) {
            assert(table_cache == nullptr);
            table_cache = new Table(file_no);
            return table_cache->reserve(size);
        }

        void scan(const uint64_t &K1, const uint64_t &K2, std::map<key_type, std::string> &key_map) {
            auto it = key_map.begin();
            TableIterator *table;
            for (auto &table_pair: cache) {
                table = table_pair.second;
                table->seek(K1, K2);
                while (table->hasNext()) {
                    key_map.insert(std::make_pair(table->key(),
                                                  std::string{table->value().data(), table->value().size()}));
                    table->next();
                }
            }
        }

        void PushCache(const char *tmp, const Option &op) {
            assert(table_cache != nullptr);
            table_cache->pushCache(tmp, op);
            auto it = new TableIterator(table_cache);
            cache.emplace(it->timestamp(), it);
            table_cache = nullptr;
        }

        std::string get(const uint64_t &key) {
            TableIterator *table;
            for (auto &it: std::ranges::reverse_view(cache)) {
                table = it.second;
                table->seek(key);
                if (table->hasNext()) {
                    return {table->value().data(), table->value().size()};
                }
            }
            return {};
        }

        ~KeyCache() {
            if (table_cache != nullptr) {
                delete table_cache;
            }
            for (auto &it: cache) {
                delete it.second;
            }
            cache.clear();
        }

        bool empty() const {
            return cache.empty();
        }

        bool GetOffset(const uint64_t &key, uint64_t &offset) {
            TableIterator *table;
            for (auto &it: std::ranges::reverse_view(cache)) {
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
        // No need to level_order, because it also needs to keep key in order
        // otherwise it sames to this one
        std::multimap<uint64_t, TableIterator *> cache;
        Table *table_cache = nullptr;
    };
}

#endif //LSMKV_SRC_KEYCACHE_H
