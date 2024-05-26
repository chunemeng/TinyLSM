#include <cstdint>
#include "./include/keycache.h"

namespace LSMKV {

    KeyCache::KeyCache(std::string db_path, Version *v) : db_name_(std::move(db_path)) {
        for (int i = 0; i < 8; ++i) {
            cache.emplace(i, std::multimap<uint64_t, TableIterator *>{});
        }
        std::vector<std::string> dirs;
        std::vector<std::string> files;
        utils::scanDirs(db_name_, dirs);
        Slice result;
        std::unique_ptr<char[]> raw;

        RandomReadableFile *file;
        bool s;
        std::string path_name = db_name_ + "/";
        size_t dir_size;
        size_t file_size;
        uint64_t dir_level;

//        if constexpr (LSMKV::Option::isIndex) {
        if (true) {
            raw = std::make_unique<char[]>(16 * 1024);
            dir_size = path_name.length();
            for (auto &dir: dirs) {
                files.clear();
                path_name.append(dir);
                path_name.append("/");
                file_size = path_name.size();
                utils::scanDir(path_name, files);
                dir_level = atoll(&dir[6]);

                if (v->NeedNewLevel(dir_level)) {
                    v->AddNewLevel(dir_level - v->GetTreeLevel());
                }

                for (auto &file_name: files) {
                    path_name.append(file_name);
                    v->LoadStatus(dir_level, stoll(file_name));
                    s = NewRandomReadableFile(path_name, &file);
                    if (!s) {
                        continue;
                    }
                    s = file->Read(0, 16 * 1024, &result, raw.get());
                    if (!s || result.empty()) {
                        delete file;
                        continue;
                    }

                    auto it = new TableIterator(new Table(result.data(), stoll(file_name)));
                    cache[dir_level].emplace(it->timestamp(), it);
//					cache.emplace_back();
                    delete file;
                    path_name.resize(file_size);
                }
                path_name.resize(dir_size);
            }

        } else {
            for (auto &dir: dirs) {
                files.clear();

                dir_size = path_name.size();
                path_name.append(dir);
                path_name.append("/");
                file_size = path_name.size();
                utils::scanDir(path_name, files
                );
                dir_level = atoll(&dir[6]);

                if (v->NeedNewLevel(dir_level)
                        ) {
                    v->AddNewLevel(dir_level - v->GetTreeLevel());
                }

                for (
                    auto &file_name
                        : files) {
                    path_name.
                            append(file_name);
                    v->
                            LoadStatus(dir_level, stoll(file_name)
                    );
                    s = NewRandomReadableFile(path_name, &file);
                    if (!s) {
                        continue;
                    }
                    if (!s || result.

                            empty()

                            ) {
                        delete
                                file;
                        continue;
                    }
                    auto it = new TableIterator(new Table(nullptr, stoll(file_name)));
                    cache[dir_level].
                            emplace(it
                                            ->

                                                    timestamp(), it

                    );
                    delete
                            file;
                    path_name.
                            resize(file_size);
                }
                path_name.
                        resize(dir_size);
            }
        }

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
        // key is level, to fix bug in the same timestamp
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
        std::priority_queue<std::multimap<uint64_t, TableIterator *>::iterator,
                std::vector<std::multimap<uint64_t, TableIterator *>::iterator>,
                CmpKey> que;

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
        for (auto rit = cache[level + 1].begin(); rit != cache[level + 1].end(); ++rit) {
            it = *rit;
            if (it.second->InRange(smallest_key, largest_key)) {
                timestamp = std::max(it.first, timestamp);
                choose_this_level.emplace((*rit).first, rit);
            }
        }
//        FindCompactionNextLevel(
//                level + 1,
//                [&](auto &rit) {
//                    it = *rit;
//                    if (it.second->InRange(smallest_key, largest_key)) {
//                        timestamp = std::max(it.first, timestamp);
//                        choose_this_level.emplace((*rit).first, rit);
//                    }
//                    return false;
//                });

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
        constexpr auto bloom_length = LSMKV::Option::bloom_size_ + 32;
        uint64_t key_offset = bloom_length;
        // todo use losertree

//  // TableIterator need to move next
//  std::vector<decltype(wait_to_merge.begin())> need_to_next;

        // the key is key
        std::map<uint64_t, std::pair<uint64_t, Slice>> merged_sst;
        std::vector<std::unique_ptr<TableIterator >> old_table;
        uint64_t key_timestamp = 0;
        old_table.reserve(wait_to_merge.size());
        for (const auto &it: wait_to_merge) {
            auto rit = (it).second;
            auto old_file_no = (rit)->file_no();
            if (file_location.count(old_file_no)) {
                old_file_nos[0].emplace_back(old_file_no);
            } else {
                old_file_nos[1].emplace_back(old_file_no);
            }
            key_timestamp = rit->timestamp();
            while (rit->hasNext()) {
                auto [iter, status] =
                        merged_sst.emplace(rit->key(), std::make_pair<uint64_t, Slice>(rit->timestamp(), rit->value()));
                if (!status && iter->second.first < key_timestamp) {
                    iter->second = std::make_pair<uint64_t, Slice>(std::move(key_timestamp), rit->value());
                }
                rit->next();
            }
            old_table.emplace_back(rit);
        }

        bool isSkip = false;
        auto t_size = merged_sst.size();
        auto it = merged_sst.begin();
        auto end = merged_sst.end();
        Slice value_slice;
        Slice tmp_slice;
        int i{};
        uint64_t wait_insert_key{};
        if (t_size >= 408) [[likely]] {
            do {
                table_cache = new Table(file_no++);
                char *tmp = table_cache->reserve(16384);
                for (i = 0; i < 408 && it != end; it++) {
                    wait_insert_key = (*it).first;
                    value_slice = (*it).second.second;
                    if (isDrop) {
                        auto len = DecodeFixed32(value_slice.data() + 8);
                        isSkip = !len;
                    }
                    if (!isSkip) [[likely]] {
                        ++i;
                        // WRITE KEY AND OFFSET
                        EncodeFixed64(tmp + key_offset, wait_insert_key);

                        memcpy(tmp + key_offset + 8, value_slice.data(), value_slice.size());
                        key_offset = key_offset + 20;
                    }
                }

                if (i == 0) {
                    // todo may bug in here
                    // the cache may leak
                    // need to test this
                    delete table_cache;
                    continue;
                }

                //WRITE HEADER
                // WRITE TIMESTAMP
                EncodeFixed64(tmp, timestamp);
                // WIRTE PAIR SIZE
                EncodeFixed64(tmp + 8, i);
                // WRITE MIN KEY
                memcpy(tmp + 16, tmp + bloom_length, 8);
                // WRITE MAX KEY
                EncodeFixed64(tmp + 24, wait_insert_key);

                memset(tmp + 32, 0, bloom_length - 32);
                CreateFilter(tmp + bloom_length, i, 20, tmp + 32);

                tmp_slice = Slice(tmp, i * 20 + bloom_length);
                // todo bug when use emplace_back
                need_to_write.emplace_back(tmp_slice);
                auto iterator = new TableIterator(table_cache);
                table_cache->pushCache(tmp_slice.data());
                cache[level + 1].emplace(timestamp, iterator);
                key_offset = bloom_length;
            } while (it != end);
            table_cache = nullptr;
            return;
        }
        table_cache = new Table(file_no++);
        char *tmp = table_cache->reserve(t_size * 20 + bloom_length);
        for (i = 0; i < 408 && it != end; it++) {
            wait_insert_key = (*it).first;
            value_slice = (*it).second.second;
            if (isDrop) {
                auto len = DecodeFixed32(value_slice.data() + 8);
                isSkip = !len;
            }
            if (!isSkip) [[likely]] {
                ++i;
                // WRITE KEY AND OFFSET
                EncodeFixed64(tmp + key_offset, wait_insert_key);

                memcpy(tmp + key_offset + 8, value_slice.data(), value_slice.size());
                key_offset = key_offset + 20;
            }
        }
        if (i == 0) {
            // todo may bug in here
            // the cache may leak
            // need to test this
            delete table_cache;
            return;
        }

        //WRITE HEADER
        // WRITE TIMESTAMP
        EncodeFixed64(tmp, timestamp);
        // WIRTE PAIR SIZE
        EncodeFixed64(tmp + 8, i);
        // WRITE MIN KEY
        memcpy(tmp + 16, tmp + bloom_length, 8);
        // WRITE MAX KEY
        EncodeFixed64(tmp + 24, wait_insert_key);

        memset(tmp + 32, 0, bloom_length - 32);
        CreateFilter(tmp + bloom_length, i, 20, tmp + 32);

        tmp_slice = Slice(tmp, i * 20 + bloom_length);
        // todo bug when use emplace_back
        need_to_write.emplace_back(tmp_slice);
        auto iterator = new TableIterator(table_cache);
        table_cache->pushCache(tmp_slice.data());
        cache[level + 1].emplace(timestamp, iterator);

        table_cache = nullptr;

//        while (!wait_to_merge.empty()) {
//            table_cache = new Table(file_no++);
//            // TODO FIND A GOOD WAY TO RESERVE
//            char *tmp = table_cache->reserve(16384);
//            Slice tmp_slice;
//            uint64_t wait_insert_key = UINT64_MAX;
//            uint64_t key_timestamp;
//            Slice value;
//            // CURRENT TABLE_CACHE.SIZE
//            uint64_t t_size = 0;
//            while (t_size < LSMKV::Option::pair_size_ && !wait_to_merge.empty()) {
//                wait_insert_key = UINT64_MAX;
//                key_timestamp = 0;
//                bool isSkip = false;
//                for (auto it = wait_to_merge.begin(); it != wait_to_merge.end(); it++) {
//                    auto rit = (*it).second;
//                    // IF THE KEY IS SMALLER THAN STORED IN WAIT_INSERT_KEY
//                    // UPDATE TO THIS ONE
//                    if (wait_insert_key > rit->key()) {
//                        need_to_next.clear();
//                        need_to_next.emplace_back(it);
//                        key_timestamp = rit->timestamp();
//                        value = rit->value();
//                        wait_insert_key = rit->key();
//                        continue;
//                    }
//                    if (wait_insert_key == rit->key()) {
//                        need_to_next.emplace_back(it);
//                        // UPDATE VALUE IF TIMESTAMP IS NEW
//                        // second is for key == uint64_max
//
//                        // BUG: KEY_TIMESTAMP MAY EQUAL RIT->TIMESTAMP()
//                        // already fixed by set ordered by reverse level
//
//                        // after a mouth, i read it again, clearly finds this just a shit
//                        // this is just access last level first
//                        // so in there only the upper timestamp will update
//                        // and will ignore the timestamp if is equal
//                        if (key_timestamp < rit->timestamp()) {
//                            key_timestamp = rit->timestamp();
//                            value = rit->value();
//                            continue;
//                        }
//
//                    }
//                }
//                if (isDrop) {
//                    auto len = DecodeFixed32(value.data() + 8);
//                    isSkip = !len;
//                }
//                if (!isSkip) [[likely]] {
//                    ++t_size;
//                    // WRITE KEY AND OFFSET
//                    EncodeFixed64(tmp + key_offset, wait_insert_key);
//
//                    memcpy(tmp + key_offset + 8, value.data(), value.size());
//                    key_offset = key_offset + 20;
//                }
//                for (auto &i: need_to_next) {
//                    auto index = (*i).second;
//                    (index)->next();
//                    if (!((index)->hasNext())) {
//                        auto it = (index)->file_no();
//                        // find the level of the file
//                        if (file_location.count(it)) {
//                            old_file_nos[0].emplace_back(it);
//                        } else {
//                            old_file_nos[1].emplace_back(it);
//                        }
//                        delete (index);
//                        // if erase first, the iterator will invalid
//                        wait_to_merge.erase(i);
//                    }
//                }
//                need_to_next.clear();
//            }
//
//            if (t_size == 0) {
//                // todo may bug in here
//                // the cache may leak
//                // need to test this
//                delete table_cache;
//                continue;
//            }
//
//            need_to_next.clear();
//            //WRITE HEADER
//            // WRITE TIMESTAMP
//            EncodeFixed64(tmp, timestamp);
//            // WIRTE PAIR SIZE
//            EncodeFixed64(tmp + 8, t_size);
//            // WRITE MIN KEY
//            memcpy(tmp + 16, tmp + bloom_length, 8);
//            // WRITE MAX KEY
//            EncodeFixed64(tmp + 24, wait_insert_key);
//
//            memset(tmp + 32, 0, bloom_length - 32);
//            CreateFilter(tmp + bloom_length, t_size, 20, tmp + 32);
//
//            tmp_slice = Slice(tmp, t_size * 20 + bloom_length);
//            // todo bug when use emplace_back
//            need_to_write.emplace_back(tmp_slice);
//            auto iterator = new TableIterator(table_cache);
////                iterator->setTimestamp(timestamp);
//            table_cache->pushCache(tmp_slice.data());
//            cache[level + 1].emplace(timestamp, iterator);
//            key_offset = bloom_length;
//        }
//        table_cache = nullptr;
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
            for (auto &table_pair: std::ranges::reverse_view(level.second)) {
                table = table_pair.second;
                table->seek(K1, K2);
                while (table->hasNext()) {
                    // First insert the key with larger timestamp
                    // if meet the same key, emplace will fail
                    key_map.emplace(table->key(),
                                    std::string{table->value().data(), table->value().size()});
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

    uint64_t BinarySearchSST(const char *start, uint64_t key, uint64_t size) {
        uint64_t left = 0, right = size, mid;
        while (left < right) {
            mid = left + ((right - left) >> 1);
            if (DecodeFixed64(start + mid * 20) < key)
                left = mid + 1;
            else
                right = mid;
        }
        // To avoid False positive
        if (left <= size && DecodeFixed64(start + left * 20) == key) {
            return left;
        }

        return size;
    }

    std::string KeyCache::get(const uint64_t &key) {
        if constexpr (!LSMKV::Option::isIndex) {
            std::vector<std::string> files;
            Slice result;
            std::unique_ptr<char[]> raw;

            RandomReadableFile *file;
            bool s;
            std::string path_name = db_name_ + "/";
            size_t dir_size = path_name.length();
            path_name += "level-0/";
            size_t file_size = path_name.length();
            uint64_t dir_level = 0;

            raw = std::make_unique<char[]>(16 * 1024);
            std::string res;

            while (utils::dirExists(path_name)) {
                utils::scanDir(path_name, files);
                uint64_t timestamp = 0;

                if (files.empty() && dir_level) {
                    return {};
                }

                for (auto &file_name: files) {
                    path_name.append(file_name);
                    s = NewRandomReadableFile(path_name, &file);
                    if (!s) {
                        continue;
                    }
                    s = file->Read(0, 16 * 1024, &result, raw.get());
                    delete file;

                    if (!s || result.empty()) {
                        path_name.resize(file_size);
                        continue;
                    }
                    auto cur_timestamp = DecodeFixed64(result.data());
                    if (cur_timestamp < timestamp) {
                        path_name.resize(file_size);
                        continue;
                    }
                    auto smallestKey = DecodeFixed64(result.data() + 16);
                    auto largestKey = DecodeFixed64(result.data() + 24);
                    if (key < smallestKey || key > largestKey || KeyMayMatch(key, result.data() + 32)) {
                        path_name.resize(file_size);
                        continue;
                    }
                    auto size = DecodeFixed64(result.data() + 8);
                    auto index = BinarySearchSST(result.data() + bloom_size, key, size);
                    if (index == size) {
                        path_name.resize(file_size);
                        continue;
                    }

                    timestamp = cur_timestamp;
                    res = std::move(std::string(result.data() + bloom_size + index * 20 + 8, 12));
                    path_name.resize(file_size);
                }

                // Found in this level, no need to search next level
                if (timestamp) {
                    return res;
                }

                dir_level++;

                path_name.resize(dir_size);
                files.clear();
//                dir_size = path_name.size();
                path_name.append("level-" + std::to_string(dir_level));
                path_name.append("/");
                file_size = path_name.size();
            }
            return {};
        }

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