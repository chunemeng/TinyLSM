#ifndef ARENA_H
#define ARENA_H

#include <cstdint>
#include <cstdlib>
#include <vector>

namespace LSMKV {

class Arena {
private:
    static constexpr int align = (sizeof(void *) > 8) ? sizeof(void *) : 8;
    constexpr static int BLOCK_SIZE = 4096;

    uint64_t waste{};
    char *alloc_ptr;
    size_t alloc_bytes_remaining;
    std::vector<char *> pool;

    char *allocateFallBack(size_t bytes) {
        if (bytes > (BLOCK_SIZE >> 2)) {
            char *result = allocateNewBlock(bytes);
            return result;
        }

        alloc_ptr = allocateNewBlock(BLOCK_SIZE);
        waste += BLOCK_SIZE - bytes;
        alloc_bytes_remaining = BLOCK_SIZE;

        char *result = alloc_ptr;
        alloc_ptr += bytes;
        alloc_bytes_remaining -= bytes;
        return result;
    }

    char *allocateNewBlock(size_t bytes);

public:
    uint64_t getWaste() const {
        return waste;
    }

    Arena() : alloc_ptr(nullptr), alloc_bytes_remaining(0){};

    ~Arena() {
        for (auto &i: pool) {
            delete[] i;
        }
    }

    char *allocate(size_t bytes);

    char *allocateAligned(size_t bytes);
};

inline char *Arena::allocate(size_t bytes) {
    if (bytes <= alloc_bytes_remaining) {
        char *result = alloc_ptr;
        alloc_ptr += bytes;
        alloc_bytes_remaining -= bytes;
        return result;
    }
    return allocateFallBack(bytes);
}

inline char *Arena::allocateNewBlock(size_t bytes) {
    char *result = new char[bytes];
    pool.push_back(result);
    return result;
}

inline char *Arena::allocateAligned(size_t bytes) {
    // alloc_ptr % align
    size_t current_mod = reinterpret_cast<uintptr_t>(alloc_ptr) & (align - 1);
    size_t slop = (current_mod == 0 ? 0 : align - current_mod);
    size_t needed = bytes + slop;
    char *result;
    if (needed <= alloc_bytes_remaining) {
        result = alloc_ptr + slop;
        alloc_ptr += needed;
        alloc_bytes_remaining -= needed;
    } else {
        result = allocateFallBack(bytes);
    }
    return result;
}
}// namespace LSMKV

#endif//ARENA_H
