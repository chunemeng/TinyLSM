#include "cache.h"


namespace LSMKV {
	// USE AS VALUE_CACHE
	struct LRUHandle {
		void* value;
		void (*deleter)(const Slice&, void* value);
		LRUHandle* next_hash;// Hash Bucket
		LRUHandle* next;
		LRUHandle* prev;
		size_t charge;
		size_t key_length;
		bool in_cache;     // Whether entry is in the cache.
		uint32_t refs;     // References, including cache reference, if present.
		uint32_t hash;     // Hash of key(); used for fast sharding and comparisons
		char key_data[1];  // Beginning of key

		[[nodiscard]] Slice key() const {
			// next is only equal to this if the LRU handle is the list head of an
			// empty list. List heads never have meaningful keys.
			assert(next != this);

			return {key_data, key_length};
		}
	};
	class LRUCache {
		static constexpr int MAX_CACHE_SIZE = 1024 * 1024 * 1024;
	public:
		LRUCache();
		~LRUCache();

		// Separate from constructor so caller can easily make an array of LRUCache
		void SetCapacity(size_t capacity) { capacity_ = capacity; }

		// Like Cache methods, but with an extra "hash" parameter.
		Cache::Handle* Insert(const Slice& key, uint32_t hash, void* value,
			size_t charge,
			void (*deleter)(const Slice& key, void* value));
		Cache::Handle* Lookup(const Slice& key, uint32_t hash);
		void Release(Cache::Handle* handle);
		void Erase(const Slice& key, uint32_t hash);
		void Prune();


	private:
		void LRU_Remove(LRUHandle* e);
		void LRU_Append(LRUHandle* list, LRUHandle* e);
		void Ref(LRUHandle* e);
		void Unref(LRUHandle* e);
		bool FinishErase(LRUHandle* e);

		size_t capacity_;

		LRUHandle lru_;

		LRUHandle in_use_;

//		HashTable table_;
	};
	Cache::Handle* LRUCache::Lookup(const Slice& key, uint32_t hash) {
		return nullptr;
	}
	void LRUCache::Erase(const Slice& key, uint32_t hash) {

	}
	void LRUCache::Prune() {

	}
	void LRUCache::LRU_Append(LRUHandle* list, LRUHandle* e) {

	}
	void LRUCache::Unref(LRUHandle* e) {

	}
	bool LRUCache::FinishErase(LRUHandle* e) {
		return false;
	}
	void LRUCache::LRU_Remove(LRUHandle* e) {

	}
	void LRUCache::Ref(LRUHandle* e) {

	}
	Cache::Handle* LRUCache::Insert(const Slice& key,
		uint32_t hash,
		void* value,
		size_t charge,
		void (* deleter)(const Slice&, void*)) {
		return nullptr;
	}
	LRUCache::LRUCache() {

	}
	LRUCache::~LRUCache() {

	}

}