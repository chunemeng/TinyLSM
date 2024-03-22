#ifndef ARENA_H
#define ARENA_H
#include <vector>
#include <cstdlib>


const static int BLOCK_SIZE = 4096;
class arena {
private:
	char* alloc_ptr;
	size_t alloc_bytes_remaining;
	std::vector<char*> pool;

	char* allocateFallBack(size_t bytes);
	char* allocateNewBlock(size_t bytes);
public:
	arena() : alloc_ptr(nullptr), alloc_bytes_remaining(0) {
	};
	~arena() {
		for (auto& i : pool) {
			delete[] i;
		}
	}
	char* allocate(size_t bytes);
	char* allocateAligned(size_t bytes);

};
inline char* arena::allocate(size_t bytes) {
	if (bytes >= alloc_bytes_remaining) {
		char* result = alloc_ptr;
		alloc_ptr += bytes;
		alloc_bytes_remaining -= bytes;
		return result;
	}
	return allocateFallBack(bytes);
}

#endif //ARENA_H
