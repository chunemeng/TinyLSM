#ifndef LSMKV_SRC_CACHE_H
#define LSMKV_SRC_CACHE_H

#include <cstdint>
#include "utils/slice.h"
#include "utils/file.h"

namespace LSMKV {
    class Cache;

    class Cache {
    public:
        Cache() = default;

        Cache(const Cache &) = delete;

        Cache &operator=(const Cache &) = delete;

        // Destroys all existing entries by calling the "deleter"
        // function that was passed to the constructor.
        ~Cache() {
            delete mmap_file_;
        }

        void Push(MemoryReadableFile *file) {
            delete mmap_file_;

            this->mmap_file_ = file;
            this->offset_ = mmap_file_->GetOffset();
            this->len_ = mmap_file_->Size();
        }

        bool Get(size_t offset, size_t len, Slice &res) {
            if (mmap_file_ == nullptr) {
                return false;
            }
            if (offset >= offset_ && offset + len <= offset_ + len_) {
                mmap_file_->Read(offset - offset_, len, &res, nullptr);
                return true;
            }
            return false;
        }

        void Drop() {
            if (mmap_file_) {
                delete mmap_file_;
                mmap_file_ = nullptr;
            }
        }

    private:
        size_t len_{};
        size_t offset_{};
        MemoryReadableFile *mmap_file_{};
    };
}
#endif //LSMKV_SRC_CACHE_H
