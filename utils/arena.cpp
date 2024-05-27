//
// Created by aa on 2024/3/22.
//

#include "arena.h"
namespace LSMKV {
	char* Arena::allocateFallBack(size_t bytes) {
		if (bytes > BLOCK_SIZE >> 2) {
			char* result = allocateNewBlock(bytes);
			return result;
		}

		alloc_ptr = allocateNewBlock(BLOCK_SIZE);
        waste += BLOCK_SIZE - bytes;
        alloc_bytes_remaining = BLOCK_SIZE;

		char* result = alloc_ptr;
		alloc_ptr += bytes;
		alloc_bytes_remaining -= bytes;
		return result;
	}

	char* Arena::allocateNewBlock(size_t bytes) {
		char* result = new char[bytes];
		pool.push_back(result);
		return result;
	}

	char* Arena::allocateAligned(size_t bytes) {
		const int align = (sizeof(void*) > 8) ? sizeof(void*) : 8;

		// alloc_ptr % align
		size_t current_mod = reinterpret_cast<uintptr_t>(alloc_ptr) & (align - 1);
		size_t slop = (current_mod == 0 ? 0 : align - current_mod);
		size_t needed = bytes + slop;
		char* result;
		if (needed <= alloc_bytes_remaining) {
			result = alloc_ptr + slop;
			alloc_ptr += needed;
			alloc_bytes_remaining -= needed;
		} else {
			result = allocateFallBack(bytes);
		}
		return result;
	}
}