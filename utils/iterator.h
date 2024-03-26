#ifndef LSM_ITERATOR_H
#define LSM_ITERATOR_H

#include <cstdint>
#include <list>
#include "slice.h"
namespace LSMKV {
	class Iterator {
	public:
		Iterator() {};

		Iterator(const Iterator&) = delete;
		Iterator& operator=(const Iterator&) = delete;

		virtual ~Iterator() {};

		virtual bool hasNext() const = 0;

		virtual void seekToFirst() = 0;

		virtual void seek(const uint64_t& target) = 0;

		virtual void next() = 0;

		virtual void scan(const uint64_t& K1,
			const uint64_t& K2,
			std::list<std::pair<uint64_t, std::string>>& list) = 0;

		virtual Slice key() const = 0;

		virtual Slice value() const = 0;
	};
}

#endif //LSM_ITERATOR_H
