#ifndef SKIPLIST_H
#define SKIPLIST_H

#include "utils/arena.h"
#include <cstdint>
#include <ctime>
#include <random>
#include <bit>
#include <atomic>

namespace LSMKV {
    static constexpr int MAX_LEVEL = 16;
    typedef unsigned char byte;

    template<typename K, typename V>
    class Skiplist {
    private:
        struct node {
            K const _key;
            V _value;
            node *_next[1];

            node *next(const byte &level) {
                return _next[level];
            }

            void setnext(const byte &l, node *n) {
                _next[l] = n;
            }

            node(const K &key, V &&value) : _key(key), _value(std::forward<V>(value)) {
            }
        };

        bool KeyIsAfterNode(const K &_key, node *n) const {
            return (n != nullptr) && (n->_key < _key);
        }

        int key_is_after_node(const K &_key, node *n) const {
            if (n == nullptr) {
                return -1;
            }
            return (n->_key < _key);
        }

        bool key_equal(const K &_key, node *n) const {
            return (_key == n->_key);
        }

        // return the node ahead of node
        node *findNode(const K &_key) const {
            node * cur = _head;
            byte level = getMaxHeight() - 1;
            while (true) {
                node * next = cur->next(level);
                if (KeyIsAfterNode(_key, next)) {
                    cur = next;
                } else {
                    if (equal(_key, next)) {
                        return next;
                    }
                    if (level == 0) {
                        return next;
                    } else {
                        level--;
                    }
                }
            }
        }

        node *find_set_prev(const K &key, node **prev) {
            node * cur = _head;
            byte level = getMaxHeight() - 1;
            while (true) {
                node * next = cur->next(level);
                switch (key_is_after_node(key, next)) {
                    case 0:
                        if (key_equal(key, next)) {
                            return next;
                        }
                        [[fallthrough]];
                    case -1:
                        prev[level] = cur;
                        if (level == 0) [[unlikely]] {
                            return next;
                        } else {
                            level--;
                        }
                        break;
                    case 1:
                        cur = next;
                        break;
                }
//                if (KeyIsAfterNode(key, next)) {
//                    cur = next;
//                } else {
//                    if (equal(key, next)) {
//                        return next;
//                    }
//                    prev[level] = cur;
//                    if (level == 0) [[unlikely]] {
//                        return next;
//                    } else {
//                        level--;
//                    }
//                }
            }
        }

        node *findHelper(const K &key, node **prev) {
            node * cur = _head;
            byte level = getMaxHeight() - 1;
            while (true) {
                node * next = cur->next(level);
                if (KeyIsAfterNode(key, next)) {
                    cur = next;
                } else {
                    if (equal(key, next)) {
                        return next;
                    }
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
            // Twice faster than upper
            return MAX_LEVEL - std::__bit_width((uint32_t) (((1 << (MAX_LEVEL - 1)) - 1) & rnd()));
        }

        [[nodiscard]] inline byte getMaxHeight() const noexcept {
            return max_level;
        }

        node *createNode(const K &key, V &&value, const byte &level) {
            auto node_memory = arena->allocateAligned(sizeof(node) + sizeof(std::atomic<node *>) * (level - 1));
            return new(node_memory) node(key, std::move(value));
        }

        std::minstd_rand rnd{std::random_device{}()};
        Arena *arena;
        byte max_level;
        node *_head;
    public:
        class Iterator {
        public:
            explicit Iterator(const Skiplist *list) : _list(list), _cur(nullptr), _end(nullptr) {};

            ~Iterator() = default;

            [[nodiscard]] bool hasNext() const {
                return _end != _cur && _cur != nullptr;
            }

            const V &value() const {
                return _cur->_value;
            }

            const K &key() const {
                return _cur->_key;
            }

            void next() {
                _cur = _cur->next(0);
            }

            void seek(const K &key) {
                _cur = _list->findNode(key);
            }

            void seek(const K &K1, const K &K2) {
                _cur = _list->findNode(K1);
                _end = _list->findNode(K2);
                if (_end != nullptr && _end->_key == K2) _end = _end->next(0);
            }

            void seekToFirst() {
                _cur = _list->_head->next(0);
            }

        private:
            // not include _end!!!!
            node *_cur;
            node *_end;
            const Skiplist *_list;
        };

        explicit Skiplist(Arena *arena)
                : arena(arena), _head(createNode(0, V(), MAX_LEVEL)), max_level(1) {
            for (byte level = 0; level < MAX_LEVEL; level++) {
                _head->setnext(level, nullptr);
            }
        };

        ~Skiplist() = default;

        V &find(const K &key) const {
            auto cur = findNode(key)->next(0);
            if (cur && cur->_key == key) {
                return cur->_value;
            }
        }

        bool insert(const K &key, V &&value) {
            node * prev[MAX_LEVEL];
            node * n = find_set_prev(key, prev);

            if (equal(key, n)) {
                n->_value = std::move(value);
                return false;
            }

            byte height = random_level();
            if (height > getMaxHeight()) {
                for (byte i = getMaxHeight(); i < height; i++) {
                    prev[i] = _head;
                }
                max_level = (height);
            }

            n = createNode(key, std::move(value), height);
            for (int i = 0; i < height; i++) {
                n->setnext(i, prev[i]->next(i));
                prev[i]->setnext(i, n);
            }
            return true;
        }

        bool equal(const K &_key, node *n) const {
            return (n != nullptr) && (_key == n->_key);
        }

        bool remove(const K &key, const V &tombstone) {
            auto prev = findNode(key);
            auto cur = prev;
            if (cur && cur->_key == key) {
                if (cur->_value == tombstone) {
                    return false;
                }
                cur->_value = tombstone;
                return true;
            }
            return false;
        }

        bool contains(const K &key) {
            auto cur = findNode(key);
            return cur && cur->_key == key;
        }

        Skiplist<K, V> &operator=(const Skiplist<K, V> &other) = delete;

        Skiplist(const Skiplist<K, V> &other) = delete;
    };
} // namespace Skiplist

#endif // SKIPLIST_H
