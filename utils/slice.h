#ifndef SLICE_H
#define SLICE_H

#include <cstddef>
#include <string>
#include <cstring>
#include <cassert>
namespace LSMKV {
	class Slice {
	public:
		Slice() : _data(""), _size(0) {
		}
		Slice(const char* data, size_t size) : _data(data), _size(size) {
		}
		Slice(const std::string& str) : _data(str.data()), _size(str.size()) {
		}
		Slice(const char* data) : _data(data), _size(data ? strlen(data) : 0) {
		}

		Slice(const Slice&) = default;
		Slice& operator=(const Slice&) = default;

		const char* data() const {
			return _data;
		}
		size_t size() const {
			return _size;
		}

		bool empty() const {
			return _size == 0;
		}

		char operator[](size_t n) const {
			assert(n < size());
			return _data[n];
		}

		void clear() {
			_data = "";
			_size = 0;
		}

		void remove_prefix(size_t n) {
			assert(n <= size());
			_data += n;
			_size -= n;
		}

		std::string toString() const {
			return { _data, _size };
		}

		int compare(const Slice& b) const;

		bool starts_with(const Slice& x) const {
			return ((_size >= x._size) && (memcmp(_data, x._data, x._size) == 0));
		}

	private:
		const char* _data;
		size_t _size;
	};

	inline bool operator==(const Slice& x, const Slice& y) {
		return ((x.size() == y.size()) &&
			(memcmp(x.data(), y.data(), x.size()) == 0));
	}

	inline bool operator<(const Slice& x, const Slice& y) {
		return (x.compare(y) < 0);
	}

	inline bool operator!=(const Slice& x, const Slice& y) {
		return !(x == y);
	}

	inline int Slice::compare(const Slice& b) const {
		// memcmp faster than strcmp and can cmp '\0'
		const size_t min_len = (_size < b._size) ? _size : b._size;
		int r = memcmp(_data, b._data, min_len);
		if (r == 0) {
			if (_size < b._size)
				r = -1;
			else if (_size > b._size)
				r = +1;
		}
		return r;
	}
}

#endif //SLICE_H
