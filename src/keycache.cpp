#include "./include/keycache.h"

namespace LSMKV {

    KeyCache::KeyCache(const std::string &db_path, Version *v) {
        for (int i = 0; i < 8; ++i) {
            cache.emplace(i, std::multimap<uint64_t, TableIterator *>{});
        }
        std::vector<std::string> dirs;
        std::vector<std::string> files;
        utils::scanDirs(db_path, dirs);
        Slice result;
        char *raw = new char[16 * 1024];
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
                if (!s.ok() || result.empty()) {
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
    }

    void KeyCache::LogLevel(uint64_t level, int i) {

        for (auto &t: cache[level]) {
            auto dit = t.second;
            dit->seekToFirst();
            auto tmm = dit->value().data();
            void *ptr = const_cast<char *>(tmm);
            while (dit->hasNext()) {
                std::cout << i << " ptr: " << ptr << "key: " << dit->key() << " offset: "
                          << DecodeFixed64(dit->value().data()) << " size: "
                          << DecodeFixed32(dit->value().data() + 8) << std::endl;
                dit->next();
            }
        }
    }

    uint64_t
    KeyCache::CompactionSST(uint64_t level, uint64_t file_no, uint64_t size, std::vector<uint64_t> *old_file_nos,
                            std::vector<uint64_t> &need_to_move, std::vector<Slice> &need_to_write) {
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
        // key is level to fix bug in the same timestamp
        std::multimap<uint64_t, TableIterator *> wait_to_merge;
        // add new level
        if (level > cache.size()) {
            size_t j = cache.size();
            for (size_t i = j; i <= level + 1; ++i) {
                cache.emplace(i, std::multimap<uint64_t, TableIterator *>{});
            }
        }
        bool isDrop = cache[level + 1].empty();


        // store the sst with same timestamp
        std::priority_queue<std::multimap<uint64_t, TableIterator *>::iterator, std::vector<std::multimap<uint64_t, TableIterator *>::iterator>, CmpKey> que;
        bool isFull = false;
        // iterator comparator
        CmpKey cmp;

        // pick sst in size
        FindCompactionNextLevel(
                level,
                [&](auto &rit) {
                    it = *rit;
                    if (isFull) {
                        // continue to find the smallest key sst with same timestamp
                        if (timestamp < it.first) {
                            while (!que.empty()) {
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
                            assert(it.first > timestamp);
                            timestamp = it.first;
                            que.push(rit);
                        }
                    }
                    return false;
                }
        );
        while (!que.empty()) {
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
            wait_to_merge.emplace(0, tmp);
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

        // START ALL ITERATOR AT START STATE
        for (auto &cit: choose_this_level) {
            tmp = (*(cit.second)).second;
            tmp->seekToFirst();
            auto t = wait_to_merge.emplace(1, tmp);
            cache[level + 1].erase(cit.second);
        }
        // START MERGE
        Merge(file_no, level, timestamp, isDrop, wait_to_merge, need_to_write, file_location,
              old_file_nos);
        return timestamp;
    }

    void KeyCache::Merge(uint64_t file_no, uint64_t level, uint64_t timestamp, bool isDrop,
                         std::multimap<uint64_t, TableIterator *> &wait_to_merge,
                         std::vector<Slice> &need_to_write, std::set<uint64_t> &file_location,
                         std::vector<uint64_t> *old_file_nos) {
        // ERROR AFTER MERGE THE SIZE MAY BE NOT SAME TO SIZE
        // file_no to generate new file
        // if the last one is full and not compaction will directly move
        // THE OFFSET START TO WRITE (BLOOM_SIZE AND HEADER_SIZE)
        uint64_t key_offset = 8224;

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
                bool isSkip = false;
                for (auto it = wait_to_merge.begin(); it != wait_to_merge.end(); it++) {
                    auto rit = (*it).second;
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

                        // BUG: KEY_TIMESTAMP MAY EQUAL RIT->TIMESTAMP()
                        // already fixed by set ordered by reverse level
                        if (key_timestamp < rit->timestamp()) {
                            key_timestamp = rit->timestamp();
                            value = rit->value();
                            continue;
                        }

                    }
                }
                if (isDrop) {
                    auto len = DecodeFixed32(value.data() + 8);
                    isSkip = !len;
                }
                if (!isSkip) [[likely]] {
                    ++t_size;
                    // WRITE KEY AND OFFSET
                    EncodeFixed64(tmp + key_offset, wait_insert_key);
                    auto t = DecodeFixed64(value.data());

                    memcpy(tmp + key_offset + 8, value.data(), value.size());
                    key_offset = key_offset + 20;
                }
                for (auto &i: need_to_next) {
                    auto index = (*i).second;
                    (index)->next();
                    if (!((index)->hasNext())) {
                        auto it = (index)->file_no();
                        // find the level of the file
                        if (file_location.count(it)) {
                            old_file_nos[0].emplace_back(it);
                        } else {
                            old_file_nos[1].emplace_back(it);
                        }
                        delete (index);
                        // if erase first, the iterator will invalid
                        wait_to_merge.erase(i);
                    }
                }
                need_to_next.clear();
            }
            if (t_size == 0) {
                continue;
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
//                iterator->setTimestamp(timestamp);
            table_cache->pushCache(tmp_slice.data(), op);
            cache[level + 1].emplace(timestamp, iterator);
            key_offset = 8224;
        }
        table_cache = nullptr;
    }

    void KeyCache::reset() {
        delete table_cache;
        for (auto &level: cache) {
            for (auto &it: level.second) {
                delete it.second;
            }
            level.second.clear();
        }
    }

    char *KeyCache::ReserveCache(size_t size, const uint64_t &file_no) {
        assert(table_cache == nullptr);
        table_cache = new Table(file_no);
        return table_cache->reserve(size);
    }

    void KeyCache::scan(const uint64_t &K1, const uint64_t &K2, std::map<key_type, std::string> &key_map) {
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

    void
    KeyCache::scan(const uint64_t &K1, const uint64_t &K2, std::map<uint64_t, std::pair<uint64_t, uint64_t>> &key_map) {
        auto it = key_map.begin();
        TableIterator *table;
        for (auto &level: cache) {
            for (auto &table_pair: level.second) {
                table = table_pair.second;
                table->seek(K1, K2);
                while (table->hasNext()) {
                    key_map.emplace(DecodeFixed64(table->value().data()),
                                    std::make_pair(table->key(), DecodeFixed32(table->value().data() + 8)));
                    table->next();
                }
            }
        }
    }

    std::string KeyCache::get(const uint64_t &key) {
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

    bool KeyCache::empty() const {
        return cache.empty();
    }

    bool KeyCache::GetOffset(const uint64_t &key, uint64_t &offset) {
        TableIterator *table;
        for (auto &level: cache) {
            for (auto &it: std::ranges::reverse_view(level.second)) {
                table = it.second;
                table->seek(key);
                if (table->hasNext()) {
                    // if newest of value is delete, no need to restore
                    if (DecodeFixed32(table->value().data() + 8) == 0) {
                        return false;
                    }
                    offset = DecodeFixed64(table->value().data());
                    return true;
                }
            }
        }
        return false;
    }

    template<typename function>
    void KeyCache::FindCompactionNextLevel(uint64_t level, const function &callback) {
        for (auto rit = cache[level].begin(); rit != cache[level].end(); ++rit) {
            if (callback(rit)) {
                break;
            }
        }
    }
}