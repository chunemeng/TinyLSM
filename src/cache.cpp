#include "include/cache.h"
#include "../MurmurHash3.h"

namespace LSMKV {
	Cache::~Cache() {}
	// USE AS VALUE_CACHE
	struct LRUHandle {
		Slice value;
		LRUHandle* next_hash;// Hash Bucket
		LRUHandle* next;
		LRUHandle* prev;
		uint32_t refs;     // References, including cache reference, if present.
		uint32_t hash;     // Hash of key(); used for fast sharding and comparisons
		uint64_t key_data;  // offset of value,
		bool in_cache;     // Whether entry is in the cache.

		~LRUHandle() {
			delete [] value.data();
		}

		uint64_t key() const {
			// next is only equal to this if the LRU handle is the list head of an
			// empty list. List heads never have meaningful keys.
			assert(next != this);

			return key_data;
		}

	};

	class HandleTable {
	public:
		HandleTable() : length_(0), elems_(0), list_(nullptr) { Resize(); }
		~HandleTable() { delete[] list_; }

		LRUHandle* Lookup(const uint64_t& key, uint32_t hash) {
			return *FindPointer(key, hash);
		}

		LRUHandle* Insert(LRUHandle* h) {
			LRUHandle** ptr = FindPointer(h->key(), h->hash);
			LRUHandle* old = *ptr;
			h->next_hash = (old == nullptr ? nullptr : old->next_hash);
			*ptr = h;
			if (old == nullptr) {
				++elems_;
				if (elems_ > length_) {
					// Since each cache entry is fairly large, we aim for a small
					// average linked list length (<= 1).
					Resize();
				}
			}
			return old;
		}

		LRUHandle* Remove(const uint64_t& key, uint32_t hash) {
			LRUHandle** ptr = FindPointer(key, hash);
			LRUHandle* result = *ptr;
			if (result != nullptr) {
				*ptr = result->next_hash;
				--elems_;
			}
			return result;
		}

	private:
		// The table consists of an array of buckets where each bucket is
		// a linked list of cache entries that hash into the bucket.
		uint32_t length_;
		uint32_t elems_;
		LRUHandle** list_;

		// Return a pointer to slot that points to a cache entry that
		// matches key/hash.  If there is no such cache entry, return a
		// pointer to the trailing slot in the corresponding linked list.
		LRUHandle** FindPointer(const uint64_t& key, uint32_t hash) {
			LRUHandle** ptr = &list_[hash & (length_ - 1)];
			while (*ptr != nullptr && ((*ptr)->hash != hash || key != (*ptr)->key())) {
				ptr = &(*ptr)->next_hash;
			}
			return ptr;
		}

		void Resize() {
			uint32_t new_length = 4;
			while (new_length < elems_) {
				new_length *= 2;
			}
			LRUHandle** new_list = new LRUHandle*[new_length];
			memset(new_list, 0, sizeof(new_list[0]) * new_length);
			uint32_t count = 0;
			for (uint32_t i = 0; i < length_; i++) {
				LRUHandle* h = list_[i];
				while (h != nullptr) {
					LRUHandle* next = h->next_hash;
					uint32_t hash = h->hash;
					LRUHandle** ptr = &new_list[hash & (new_length - 1)];
					h->next_hash = *ptr;
					*ptr = h;
					h = next;
					count++;
				}
			}
			assert(elems_ == count);
			delete[] list_;
			list_ = new_list;
			length_ = new_length;
		}
	};
	class LRUCache {
		static constexpr int MAX_CACHE_SIZE = 1024 * 1024 * 1024;
	public:
		LRUCache();
		~LRUCache();

		void SetCapacity(size_t capacity) { capacity_ = capacity; }

		Cache::Handle* Insert(const uint64_t& key, uint32_t hash, Slice value);
		Cache::Handle* Lookup(const uint64_t& key, uint32_t hash);
		void Release(Cache::Handle* handle);
		void Erase(const uint64_t& key, uint32_t hash);
		void Prune();
		size_t TotalCharge() const {
			return usage_;
		}

	private:
		void LRU_Remove(LRUHandle* e);
		void LRU_Append(LRUHandle* list, LRUHandle* e);
		void Ref(LRUHandle* e);
		void Unref(LRUHandle* e);
		bool FinishErase(LRUHandle* e);



		size_t capacity_;
		size_t usage_;


		LRUHandle lru_;

		LRUHandle in_use_;

		HandleTable table_;
	};
	Cache::Handle* LRUCache::Lookup(const uint64_t & key, uint32_t hash) {
		LRUHandle* e = table_.Lookup(key, hash);
		if (e != nullptr) {
			Ref(e);
		}
	}
	void LRUCache::Erase(const uint64_t& key, uint32_t hash) {
		FinishErase(table_.Remove(key, hash));
	}
	void LRUCache::Prune() {
		while (lru_.next != &lru_) {
			LRUHandle* e = lru_.next;
			assert(e->refs == 1);
			bool erased = FinishErase(table_.Remove(e->key(), e->hash));
			if (!erased) {  // to avoid unused variable when compiled NDEBUG
				assert(erased);
			}
		}
	}
	void LRUCache::LRU_Append(LRUHandle* list, LRUHandle* e) {
		e->next = list;
		e->prev = list->prev;
		e->prev->next = e;
		e->next->prev = e;
	}
	void LRUCache::Unref(LRUHandle* e) {

	}
	bool LRUCache::FinishErase(LRUHandle* e) {
		if (e != nullptr) {
			assert(e->in_cache);
			LRU_Remove(e);
			e->in_cache = false;
			usage_ -= e->value.size();
			Unref(e);
		}
		return e != nullptr;
	}
	void LRUCache::LRU_Remove(LRUHandle* e) {
		e->next->prev = e->prev;
		e->prev->next = e->next;
	}
	void LRUCache::Ref(LRUHandle* e) {
		if (e->refs == 1 && e->in_cache) {  // If on lru_ list, move to in_use_ list.
			LRU_Remove(e);
			LRU_Append(&in_use_, e);
		}
		e->refs++;
	}
	Cache::Handle* LRUCache::Insert(const uint64_t& key,
		uint32_t hash,
		Slice value) {
		LRUHandle* e = new LRUHandle();
		e->value = value;
		e->hash = hash;
		e->in_cache = false;
		e->refs = 1;  // for the returned handle.

		if (capacity_ > 0) {
			e->refs++;  // for the cache's reference.
			e->in_cache = true;
			LRU_Append(&in_use_, e);
			usage_ += value.size();
			FinishErase(table_.Insert(e));
		} else {  // don't cache. (capacity_==0 is supported and turns off caching.)
			// next is read by key() in an assert, so it must be initialized
			e->next = nullptr;
		}
		while (usage_ > capacity_ && lru_.next != &lru_) {
			LRUHandle* old = lru_.next;
			assert(old->refs == 1);
			bool erased = FinishErase(table_.Remove(old->key(), old->hash));
			if (!erased) {  // to avoid unused variable when compiled NDEBUG
				assert(erased);
			}
		}

		return reinterpret_cast<Cache::Handle*>(e);
	}
	LRUCache::LRUCache(): capacity_(0), usage_(0) {
		// Make empty circular linked lists.
		lru_.next = &lru_;
		lru_.prev = &lru_;
		in_use_.next = &in_use_;
		in_use_.prev = &in_use_;
	}
	LRUCache::~LRUCache() {

	}
	void LRUCache::Release(Cache::Handle* handle) {
		Unref(reinterpret_cast<LRUHandle*>(handle));
	}

	static const int kNumShardBits = 4;
	static const int kNumShards = 1 << kNumShardBits;

	class ShardedLRUCache : public Cache {
	private:
		LRUCache shard_[kNumShards];
		uint64_t last_id_;

		static inline uint32_t HashSlice(const uint64_t& s) {
			uint32_t hash[4] = {0};
			MurmurHash3_x64_128(&s, 8, 1,&hash);
			return hash[3];
		}

		static uint32_t Shard(uint32_t hash) { return hash >> (32 - kNumShardBits); }

	public:
		explicit ShardedLRUCache(size_t capacity) : last_id_(0) {
			const size_t per_shard = (capacity + (kNumShards - 1)) / kNumShards;
			for (int s = 0; s < kNumShards; s++) {
				shard_[s].SetCapacity(per_shard);
			}
		}
		~ShardedLRUCache() override {}
		Handle* Insert(const uint64_t& key, const Slice& value) override {
			const uint32_t hash = HashSlice(key);
			return shard_[Shard(hash)].Insert(key, hash, value);
		}
		Handle* Lookup(const uint64_t& key) override {
			const uint32_t hash = HashSlice(key);
			return shard_[Shard(hash)].Lookup(key, hash);
		}
		void Release(Handle* handle) override {
			LRUHandle* h = reinterpret_cast<LRUHandle*>(handle);
			shard_[Shard(h->hash)].Release(handle);
		}
		void Erase(const uint64_t& key) override {
			const uint32_t hash = HashSlice(key);
			shard_[Shard(hash)].Erase(key, hash);
		}
		const char* Value(Handle* handle) override {
			return reinterpret_cast<LRUHandle*>(handle)->value.data();
		}
		uint64_t NewId() override {
			return ++(last_id_);
		}
		void Prune() override {
			for (int s = 0; s < kNumShards; s++) {
				shard_[s].Prune();
			}
		}
		size_t TotalCharge() const override {
			size_t total = 0;
			for (int s = 0; s < kNumShards; s++) {
				total += shard_[s].TotalCharge();
			}
			return total;
		}
	};
Cache* NewLRUCache(size_t capacity) { return new ShardedLRUCache(capacity); }

}