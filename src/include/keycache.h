#ifndef LSMKV_SRC_KEYCACHE_H
#define LSMKV_SRC_KEYCACHE_H

#include <list>
#include <queue>
#include "memtable.h"
#include "table.h"
#include "../../utils.h"
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

        explicit KeyCache(const std::string &db_path, Version *v);

        void LogLevel(uint64_t level, int i);


        // return timestamp to update the version
        uint64_t CompactionSST(uint64_t level, uint64_t file_no, uint64_t size,
                               std::vector<uint64_t> old_file_nos[2],
                               std::vector<uint64_t> &need_to_move,
                               std::vector<Slice> &need_to_write
        );

        template<typename function>
        void FindCompactionNextLevel(uint64_t level,
                                     function const &callback);

        void Merge(uint64_t file_no,
                   uint64_t level,
                   uint64_t timestamp, bool isDrop,
                   std::multimap<uint64_t, TableIterator *> &wait_to_merge,
                   std::vector<Slice> &need_to_write,
                   std::set<uint64_t> &file_location,
                   std::vector<uint64_t> old_file_nos[2]);

        void reset();

        char *ReserveCache(size_t size, const uint64_t &file_no);

        void scan(const uint64_t &K1, const uint64_t &K2, std::map<key_type, std::string> &key_map);

        void scan(const uint64_t &K1, const uint64_t &K2, std::map<uint64_t, std::pair<uint64_t, uint64_t>> &key_map);

        void PushCache(const char *tmp, const Option &op) {
            assert(table_cache != nullptr);
            table_cache->pushCache(tmp, op);
            auto it = new TableIterator(table_cache);
            cache[0].emplace(it->timestamp(), it);
            table_cache = nullptr;
        }

        std::string get(const uint64_t &key);

        ~KeyCache() {
            delete table_cache;

            for (auto &level: cache) {
                for (auto &it: level.second) {
                    delete it.second;
                }
            }
            cache.clear();
        }

        [[nodiscard]] bool empty() const;

        bool GetOffset(const uint64_t &key, uint64_t &offset);

    private:
        // key is timestamp
        // No need to level_order, because it also needs to keep key in order
        // otherwise it sames to this one
        // key is level , value's key is timestamp
        Option &op = Option::getInstance();
        std::map<uint64_t, std::multimap<uint64_t, TableIterator *>> cache;
        Table *table_cache = nullptr;

        // the comparator of TableIterator*
        struct CmpKey {
            bool operator()(const std::multimap<uint64_t, TableIterator *>::iterator &left,
                            const std::multimap<uint64_t, TableIterator *>::iterator &right) {
                return left->second->SmallestKey() < right->second->SmallestKey();
            }
        };
    };
}

#endif //LSMKV_SRC_KEYCACHE_H
