#ifndef BUILDER_H
#define BUILDER_H

#include <string>
#include "status.h"
#include "../../utils/filemeta.h"
#include "version.h"
#include "keycache.h"

namespace LSMKV {
    class Iterator;


    Status BuildTable(const std::string &dbname, Version *v, Iterator *iter, FileMeta &file, KeyCache *kc);

    Status SSTCompaction(uint64_t level, uint64_t file_no, Version *v, KeyCache *kc);

    Status MoveToNewLevel(uint64_t level, const uint64_t &timestamp, std::vector<uint64_t> &new_files, Version *v);

    Status WriteSlice(std::vector<Slice> &need_to_write, uint64_t level, Version *v);

    uint64_t FindLevels(const std::string &dbname, Version *v);

    struct Builder {
        void operator()() const {
            FileMeta meta;
            meta.size = size_;
            BuildTable(dbname_, v_, it_, meta, kc_);
        }

        void setAll(size_t size, Version *v, Iterator *it, KeyCache *kc) {
            this->size_ = size;
            this->it_ = it;
            this->kc_ = kc;
            this->v_ = v;
        }

        size_t size_;
        const char *dbname_;
        Version *v_;
        Iterator *it_;
        KeyCache *kc_;
    };
}

#endif //BUILDER_H
