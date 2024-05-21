#ifndef BUILDER_H
#define BUILDER_H

#include <string>
#include "../../utils/filemeta.h"
#include "version.h"
#include "keycache.h"

namespace LSMKV {
    class Iterator;


    bool BuildTable(const std::string &dbname, Version *v, Iterator *iter, size_t size, KeyCache *kc);

    bool SSTCompaction(uint64_t level, uint64_t file_no, Version *v, KeyCache *kc);

    bool MoveToNewLevel(uint64_t level, const uint64_t &timestamp, std::vector<uint64_t> &new_files, Version *v);

    bool WriteSlice(std::vector<Slice> &need_to_write, uint64_t level, Version *v);

    uint64_t FindLevels(const std::string &dbname, Version *v);

    struct Builder {
        explicit Builder(const char *dbname, Version *v, KeyCache *kc) : dbname_(dbname), v_(v), kc_(kc) {
        }

        void operator()() const {
            BuildTable(dbname_, v_, it_, size_, kc_);
        }

        void setAll(size_t size, Iterator *it) {
            this->size_ = size;
            this->it_ = it;
        }

        size_t size_{};
        Iterator *it_{};
        const char *dbname_;
        Version *v_;
        KeyCache *kc_;
    };
}

#endif //BUILDER_H
