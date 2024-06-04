#include <cstdint>
#include "./include/keycache.h"

namespace LSMKV {

    KeyCache::KeyCache(std::string db_path, Version *v) : db_name_(std::move(db_path)) {
        cache.reserve(16);
        for (int i = 0; i < 8; ++i) {
            cache.emplace_back();
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
        uint64_t cur_level = 7;

        //        if constexpr (LSMKV::Option::isIndex) {
        if constexpr (true) {
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
                if (dir_level > cur_level) {
                    for (; cur_level <= dir_level; ++cur_level) {
                        cache.emplace_back();
                    }
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
                    if (!s || result.

                            empty()

                            ) {
                        delete file;
                        continue;
                    }
                    auto it = new TableIterator(new Table(nullptr, stoll(file_name)));
                    cache[dir_level].emplace(it->

                            timestamp(), it

                    );
                    delete file;
                    path_name.resize(file_size);
                }
                path_name.resize(dir_size);
            }
        }

    }

    KeyCache::~KeyCache() {
        delete table_cache;

        for (const auto &level: cache) {
            for (const auto &it: level) {
                delete it.second;
            }
        }
        cache.clear();
    }

    void KeyCache::LogLevel(uint64_t level, int i) {

        for (auto &t: cache[level]) {
            auto dit = t.second;
            dit->seekToFirst();
            auto tmm = dit->value().data();
            void *ptr = const_cast<char *>(tmm);
            while (dit->hasNext()) {
                std::cout << i << " ptr: " << ptr << "key: " << dit->key() << " offset: "
                          << DecodeFixed64(dit->value().data()) << " size: " << DecodeFixed32(dit->value().data() + 8)
                          << std::endl;
                dit->next();
            }
        }
    }

    uint64_t KeyCache::CompactionSST(uint64_t level,
                                     uint64_t file_no,
                                     uint64_t size,
                                     std::vector<uint64_t> *old_file_nos,
                                     std::vector<uint64_t> &need_to_move,
                                     std::vector<WriteSlice> &need_to_write) {
        // NEED LEVEL-N AND LEVEL-N+1 FILE_NOS
        // TODO CHANGE TO UNORDERED_MAP(?)

        // key is timestamp, value is iterator
        std::multimap<uint64_t, std::multimap<uint64_t, TableIterator *>::iterator> choose_this_level;
//         tmp value
//        auto choose_it = choose_this_level.begin();
        std::pair<uint64_t, TableIterator *> it;
        // size of sst need to read
        TableIterator *tmp;
        // timestamp need to update
        uint64_t timestamp = 0;

        // STRANGE BUG THAT CANT INSERT INTO UNORDERED_SET
        // key is level, to fix bug in the same timestamp
        std::vector<std::unique_ptr<TableIterator>> wait_to_merge;
        // add new level
        if (level > cache.size()) [[unlikely]] {
            size_t j = cache.size();
            for (size_t i = j; i <= level + 1; ++i) {
                cache.emplace_back();
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
        for (auto rit = cache[level].begin(); rit != cache[level].end(); ++rit) {
            it = *rit;
            if (isFull) {
                // continue to find the smallest key sst with same timestamp
                if (timestamp < it.first) {
                    while (!que.empty()) {
                        choose_this_level.emplace(timestamp, que.top());
                        que.pop();
                    }
                    break;
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
        }

//        FindCompactionNextLevel(level, [&](auto &rit) {
//            it = *rit;
//            if (isFull) {
//                // continue to find the smallest key sst with same timestamp
//                if (timestamp < it.first) {
//                    while (!que.empty()) {
//                        choose_this_level.emplace(timestamp, que.top());
//                        que.pop();
//                    }
//                    return true;
//                } else {
//                    if (cmp(rit, que.top())) {
//                        que.pop();
//                        que.push(rit);
//                    }
//                }
//            } else {
//                if (size - 1 == choose_this_level.size() + que.size()) {
//                    isFull = true;
//                }
//                if (it.first == timestamp) {
//                    que.push(rit);
//                } else {
//                    while (!que.empty()) {
//                        choose_this_level.emplace(timestamp, que.top());
//                        que.pop();
//                    }
//                    assert(it.first > timestamp);
//                    timestamp = it.first;
//                    que.push(rit);
//                }
//            }
//            return false;
//        });

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
            tmp = ((cit.second))->second;
            smallest_key = std::min(smallest_key, tmp->SmallestKey());
            largest_key = std::max(largest_key, tmp->LargestKey());
            old_file_nos[0].emplace_back(tmp->file_no());
            tmp->seekToFirst();
            wait_to_merge.emplace_back(tmp);
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

        // START ALL ITERATOR AT START STATE
        for (auto &cit: choose_this_level) {
            tmp = (*(cit.second)).second;
            tmp->seekToFirst();
            old_file_nos[1].emplace_back(tmp->file_no());
            wait_to_merge.emplace_back(tmp);
            cache[level + 1].erase(cit.second);
        }
        // START MERGE
        Merge(file_no, level, timestamp, isDrop, wait_to_merge, need_to_write);
        return timestamp;
    }

    // key = U64_MAX and timestamp = 0 is end
    // note: the tuple is <key,timestamp,value>
    int cmp(Table::TableIterator *lhs, Table::TableIterator *rhs) {
        return lhs->merge_key() < rhs->merge_key();
        //		if (lhs->merge_key() != rhs->merge_key()) {
        //			return lhs->key() > rhs->key() ? 1 : -1;
        //		}
        //		if (lhs->timestamp() != rhs->timestamp()) {
        //			return lhs->timestamp() > rhs->timestamp() ? 2 : -2;
        //		}
        //		return 0;
    }

    struct LoserTree {
        LoserTree(size_t n, std::vector<std::unique_ptr<Table::TableIterator>> &m_way) : n_(n), m_way_(m_way) {
            std::vector<size_t> winner;
            winner.reserve(n);
            winner.resize(n);
            size_t m_way_init = (n) / 2;
            size_t child;
            size_t i;
            for (i = n - 1; i > m_way_init; --i) {
                child = i * 2 - n;
                set_loser_winner(loser_[i], winner[i], child, child + 1);
            }

            if (n & 1) { // odd
                // left child is last inner node, right child is first external node
                set_loser_winner(loser_[m_way_init], winner[m_way_init], winner[n - 1], 0);
            } else {
                set_loser_winner(loser_[m_way_init], winner[m_way_init], 0, 1);
            }

            for (i = m_way_init; i > 0; i /= 2)
                for (size_t j = i; j > i / 2;) {
                    --j;
                    set_loser_winner(loser_[j], winner[j], winner[2 * j], winner[2 * j + 1]);
                }
            loser_[0] = winner[1];
        }

        bool end() {
            return m_way_[top()]->timestamp() == 0;
        }

        void Adjust(size_t index) {
            size_t parent = index + n_;
            Table::TableIterator *s = m_way_[index].get();
            Table::TableIterator *l;

            size_t *ptree = loser_;
            while ((parent /= 2) > 0) {
                size_t &tparent = ptree[parent];
                if (!cmp(s, l = m_way_[tparent].get())) {
                    std::swap(tparent, index);
                    std::swap(s, l);
                }
            }
            loser_[0] = index;
        }

        static LoserTree *CreateLoser(size_t n, std::vector<std::unique_ptr<Table::TableIterator>> &m_way) {
            void *ptr = malloc(sizeof(LoserTree) + sizeof(size_t) * n);
            return new(ptr) LoserTree(n, m_way);
        }

        size_t top() {
            return loser_[0];
        }

        [[nodiscard]] auto top_iter(size_t& top) const{
            top = loser_[0];
            return m_way_[top].get();
        }

        void increment() {
            size_t top = loser_[0];
            m_way_[top]->merge_next();
            Adjust(top);
        }

        void set_loser_winner(size_t &loser, size_t &winner, size_t left, size_t right) {
            if (cmp(m_way_[left].get(), m_way_[right].get())) {
                loser = right;
                winner = left;
            } else {
                loser = left;
                winner = right;
            }
        }

        ~LoserTree() = default;

        size_t n_;
        std::vector<std::unique_ptr<Table::TableIterator>> &m_way_;
        // key[0] wait to insert
        size_t loser_[0];
    };


    void KeyCache::Merge(uint64_t file_no,
                         uint64_t level,
                         uint64_t timestamp,
                         bool isDrop,
                         std::vector<std::unique_ptr<TableIterator>> &wait_to_merge,
                         std::vector<WriteSlice> &need_to_write) {
        // ERROR AFTER MERGE THE SIZE MAY BE NOT SAME TO SIZE
        // file_no to generate new file
        // if the last one is full and not compaction will directly move
        // THE OFFSET START TO WRITE (BLOOM_SIZE AND HEADER_SIZE)
        constexpr auto bloom_length = LSMKV::Option::bloom_size_ + 32;
        uint64_t key_offset = bloom_length;

        //  // TableIterator need to move next
        //  std::vector<decltype(wait_to_merge.begin())> need_to_next;

        // the key is key
        std::map<uint64_t, std::pair<uint64_t, Slice>> merged_sst;
        uint64_t key_timestamp = 0;
        auto n = wait_to_merge.size();
        // todo n == 1 ?????
        if (n <= 2) [[unlikely]] {
            for (const auto &rit: wait_to_merge) {
                key_timestamp = rit->timestamp();
                while (rit->hasNext()) {
                    auto [iter, status] =
                            merged_sst.emplace(rit->key(),
                                               std::make_pair<uint64_t, Slice>(rit->timestamp(), rit->value()));
                    if (!status && iter->second.first < key_timestamp) {
                        iter->second = std::make_pair<uint64_t, Slice>(std::move(key_timestamp), rit->value());
                    }
                    rit->next();
                }
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

                            memcpy_tiny(tmp + key_offset + 8, value_slice.data(), value_slice.size());
                            key_offset = key_offset + 20;
                        }
                    }

                    if (i == 0) {
                        // todo may bug in here
                        // the cache may leak
                        // need to test this
                        delete table_cache;
                        table_cache = nullptr;
                        break;
                    }

                    //WRITE HEADER
                    // WRITE TIMESTAMP
                    EncodeFixed64(tmp, timestamp);
                    // WIRTE PAIR SIZE
                    EncodeFixed64(tmp + 8, i);
                    // WRITE MIN KEY
                    memcpy_tiny(tmp + 16, tmp + bloom_length, 8);
                    // WRITE MAX KEY
                    EncodeFixed64(tmp + 24, wait_insert_key);

                    tmp_slice = Slice(tmp, i * 20 + bloom_length);
                    // todo bug when use emplace_back
                    need_to_write.emplace_back(tmp, i * 20 + bloom_length);
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

                    memcpy_tiny(tmp + key_offset + 8, value_slice.data(), value_slice.size());
                    key_offset = key_offset + 20;
                }
            }
            if (i == 0) {
                // todo may bug in here
                // the cache may leak
                // need to test this
                delete table_cache;
                table_cache = nullptr;
                return;
            }

            //WRITE HEADER
            // WRITE TIMESTAMP
            EncodeFixed64(tmp, timestamp);
            // WIRTE PAIR SIZE
            EncodeFixed64(tmp + 8, i);
            // WRITE MIN KEY
            memcpy_tiny(tmp + 16, tmp + bloom_length, 8);
            // WRITE MAX KEY
            EncodeFixed64(tmp + 24, wait_insert_key);

            tmp_slice = Slice(tmp, i * 20 + bloom_length);
            // todo bug when use emplace_back
            need_to_write.emplace_back(tmp, i * 20 + bloom_length);
            auto iterator = new TableIterator(table_cache);
            table_cache->pushCache(tmp_slice.data());
            cache[level + 1].emplace(timestamp, iterator);

            table_cache = nullptr;
        } else {
            // init loser
            LoserTree *loser = LoserTree::CreateLoser(n, wait_to_merge);
            TableIterator *iter;
            size_t index;
            size_t old_index;
            uint64_t wait_insert_key{};
            uint64_t cur_timestamp;
            int i;
            Slice value_slice;
            do {
                cur_timestamp = 0;
                if (loser->end()) [[unlikely]] {
                    break;
                }
                table_cache = new Table(file_no++);
                char *tmp = table_cache->reserve(16384);
                for (i = 0;; loser->increment()) {
//                    index = loser->top();
//                    iter = wait_to_merge[index].get();

                    iter = loser->top_iter(index);

                    if (iter->timestamp() == 0) [[unlikely]] {
                        break;
                    }

                    if (cur_timestamp != 0 && (wait_insert_key == iter->key())) {
                        if (cur_timestamp > iter->timestamp()) {
                            continue;
                        } else if (cur_timestamp == iter->timestamp()) [[unlikely]] {
                            if (old_index < index) {
                                continue;
                            }
                        }
                        key_offset -= 20;
                        i--;
                    }

                    if (i == 408) [[unlikely]] {
                        break;
                    }

                    wait_insert_key = iter->key();

                    value_slice = iter->value();
                    old_index = index;
                    cur_timestamp = iter->timestamp();

                    if (!isDrop || DecodeFixed32(value_slice.data() + 8)) [[likely]] {
                        ++i;
                        // WRITE KEY AND OFFSET
                        EncodeFixed64(tmp + key_offset, wait_insert_key);

                        memcpy_tiny(tmp + key_offset + 8, value_slice.data(), value_slice.size());
                        key_offset += 20;
                    }
                }

//                if (i == 0) [[unlikely]] {
//                    std::cout<<"heloo"<<std::endl;
//                    delete table_cache;
//                    table_cache = nullptr;
//                    file_no--;
//                    loser->m_way_.clear();
//                    free(loser);
//                    return;
//                }

                EncodeFixed64(tmp, timestamp);
                // WIRTE PAIR SIZE
                EncodeFixed64(tmp + 8, i);
                // WRITE MIN KEY
                memcpy_tiny(tmp + 16, tmp + bloom_length, 8);
                // WRITE MAX KEY
                EncodeFixed64(tmp + 24, wait_insert_key);

                Slice tmp_slice = Slice(tmp, i * 20 + bloom_length);
                // todo bug when use emplace_back
                need_to_write.emplace_back(tmp, i * 20 + bloom_length);
                auto iterator = new TableIterator(table_cache);
                //                iterator->setTimestamp(timestamp);
                table_cache->pushCache(tmp_slice.data());
                cache[level + 1].emplace(timestamp, iterator);
                key_offset = bloom_length;
            } while (cur_timestamp);
            table_cache = nullptr;
            loser->m_way_.clear();
            free(loser);
        }
    }

    void KeyCache::reset() {
        delete table_cache;
        for (auto &level: cache) {
            for (auto &it: level) {
                delete it.second;
            }
            level.clear();
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
        for (const auto &level: cache) {
            for (auto &table_pair: std::ranges::reverse_view(level)) {
                table = table_pair.second;
                table->seek(K1, K2);
                while (table->hasNext()) {
                    // First insert the key with larger timestamp
                    // if meet the same key, emplace will fail
                    key_map.emplace(table->key(), std::string{table->value().data(), table->value().size()});
                    table->next();
                }
            }
        }
    }

    void KeyCache::scan(const uint64_t &K1,
                        const uint64_t &K2,
                        std::map<uint64_t, std::pair<uint64_t, uint64_t>> &key_map) {
        auto it = key_map.begin();
        TableIterator *table;
        for (const auto &level: cache) {
            for (auto &table_pair: level) {
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
        for (const auto &level: cache) {
            for (auto &it: std::ranges::reverse_view(level)) {
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
            for (const auto &it: std::ranges::reverse_view(level)) {
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