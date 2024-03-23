#ifndef SKIPLIST_H
#define SKIPLIST_H

#include <cstdint>
#include <string>
#include "utils/arena.h"

namespace skiplist {
#define MAX_LEVEL 18
	typedef unsigned char byte;
	template<typename K, class Comparator>
	class skiplist {
	private:
		struct node {
			K _key;
			node* _next[1];
			node* next(const byte& level) {
				return _next[level];
			}
			void setnext(const byte& l, node* n) {
				_next[l] = n;
			}
			explicit node(K&& key) : _key(std::forward<K>(key)){
			}
		};

		bool KeyIsAfterNode(const K& _key, node* n) const {
			return (n != nullptr) && (n->_key < _key);
		}

		node* findNode(const K& _key) const {
			node* cur = _head;
			byte level = getMaxHeight() - 1;
			while (true) {
				node* next = cur->next(level);
				if (KeyIsAfterNode(_key, next)) {
					cur = next;
				} else {
					if (level == 0) {
						return next;
					} else {
						level--;
					}
				}
			}
		}

		node* findHelper(const K& key, node** prev) {
			node* cur = _head;
			byte level = getMaxHeight() - 1;
			while (true) {
				node* next = cur->next(level);
				if (KeyIsAfterNode(key, next)) {
					cur = next;
				} else {
					if (prev != nullptr) prev[level] = cur;
					if (level == 0) {
						return next;
					} else {
						level--;
					}
				}
			}
		}
		byte random_level() {
			byte level = 1;
			while (!(std::rand() & 4) && ++level < MAX_LEVEL) {
			}
			return level;
		}
		inline byte getMaxHeight() const {
			return max_level;
		}
		node* createNode(K&& key, const byte& level) {
			auto node_memory = arena->allocateAligned(sizeof(node) + sizeof(node*) * (level - 1));
			return new(node_memory) node(std::forward<K>(key));
		}

		arena* arena;
		const Comparator comparator;
		byte max_level;
		node* _head;
	public:
		skiplist(double p) : _head(createNode(0, "", MAX_LEVEL)), prob(p), max_level(1) {
			for (byte level = 0; level < MAX_LEVEL; level++) {
				_head->setnext(level, nullptr);
			}
		};
		skiplist(double p, class arena* are)
			: arena(are), _head(createNode(0, "", MAX_LEVEL)), prob(p), max_level(1) {
			for (byte level = 0; level < MAX_LEVEL; level++) {
				_head->setnext(level, nullptr);
			}
		};

		V& find(const K& key) const {
			auto cur = findNode(key)->next(0);
			if (cur && cur->_key == key) {
				return cur->_value;
			}
		}
		int query(const K& key) const {
			int result = 0;
			node* cur = _head;
			byte level = getMaxHeight() - 1;
			while (true) {
				++result;
				node* next = cur->next(level);
				if (KeyIsAfterNode(key, next)) {
					cur = next;
				} else {
					if (equal(key,next)) {
						return result;
					}
					if (level == 0) {
						return result;
					} else {
						level--;
					}
				}
			}
		}
		void insert(const K& key, V&& value) {
			node* prev[MAX_LEVEL];
			node* n = findHelper(key, prev);

			byte height = random_level();
			if (height > getMaxHeight()) {
				for (byte i = getMaxHeight(); i < height; i++) {
					prev[i] = _head;
				}
				max_level = height;
			}

			n = createNode(key, std::forward<V>(value), height);
			for (int i = 0; i < height; i++) {
				n->setnext(i, prev[i]->next(i));
				prev[i]->setnext(i, n);
			}
		}
		bool equal(const K& _key, node* n) const {
			return (n != nullptr) && (_key == n->_key);
		}

/*	bool remove(const K& key) {
		auto prev = findHelper(key);
		auto cur = prev->next[0];
		if (cur && cur->_key == key) {
			prev->_next = cur->_next;
			cur->_next = nullptr;
			delete cur;
			return true;
		}
		return false;
	}*/

		bool contains(const K& key) {
			auto cur = findHelper(key, nullptr);
			return cur && cur->_key == key;
		}

		skiplist<K, V>& operator=(const skiplist<K, V>& other) = delete;
		skiplist(const skiplist<K, V>& other) = delete;
	};
	using key_type = uint64_t;
	// using value_type = std::vector<char>;
	using value_type = std::string;

	class skiplist_type {
		// add something here
	private:
		arena* const a;
		skiplist<key_type, value_type> table;
	public:
		explicit skiplist_type(double p = 0.5);
		void put(key_type key, const value_type& val);
		void put(key_type key, value_type&& val);
		//std::optional<value_type> get(key_type key) const;
		std::string get(key_type key) const;
		// for hw1 only
		int query_distance(key_type key) const;
	};

} // namespace skiplist

#endif // SKIPLIST_H
