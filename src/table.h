#ifndef LSMKV_SRC_TABLE_H
#define LSMKV_SRC_TABLE_H
#include <string>
#include "memtable.h"
#include "../utils/option.h"
#include "../utils/coding.h"
#include "../utils/bloomfilter.h"
namespace LSMKV {
	//instance of sst
	static constexpr uint64_t bloom_size = 8192;
	class Table {
	public:
		explicit Table() {
		}
		char* reserve(size_t size) {
			return arena.allocate(size);
		}
		explicit Table(const char* tmp, Option option) {
			if ((isFilter = option.isFilter)) {
				char* bloomer = arena.allocate(bloom_size);
				strncpy(bloomer, tmp + 32, 8192);
				bloom = Slice(bloomer, 8192);
			}
			timestamp = DecodeFixed64(tmp);
			uint64_t size = DecodeFixed64(tmp + 8);
			uint64_t offset = bloom_size + 32;

			// TODO MAY NEED COMPARE!!
			Slice value_offset;
			sst.reserve(size * 20);
			char* buf = arena.allocate(size * 20);
			memcpy(buf, tmp + 8224, size * 20);
			for (uint64_t i = 0; i < size * 20; i += 20) {
				value_offset = Slice(tmp + offset + 8 + i, 12);
				sst.emplace_back(DecodeFixed64(tmp + offset + i), value_offset);

			}
		}
		void pushCache(const char* tmp) {
			bloom = Slice(tmp + 32, 8192);
			timestamp = DecodeFixed64(tmp);
			uint64_t size = DecodeFixed64(tmp + 8);
			uint64_t offset = bloom_size + 32;

			// TODO MAY NEED COMPARE!!
			sst.reserve(size * 20);
			for (uint64_t i = 0; i < size * 20; i += 20) {
				// NO NEED TO ALLOCATE NEW MEMORY
				sst.emplace_back(DecodeFixed64(tmp + offset + i), std::move(Slice(tmp + offset + 8 + i, 12)));
			}
		}
		[[nodiscard]] uint64_t GetTimestamp() const {
			return timestamp;
		}

		bool KeyMatch(const uint64_t& key) {
			return KeyMayMatch(key, bloom);
		}

		~Table() {
			if (isFilter) {
				delete[] bloom.data();
			}
		}

		bool operator<(const Table& rhs) const {
			return timestamp < rhs.timestamp;
		}

		bool operator<(const uint64_t& rhs) const {
			return timestamp < rhs;
		}

		std::string get(const uint64_t& key) {
			if (!KeyMatch(key)) {
				return "";
			}
			uint64_t offset = BinarySearchGet(0, sst.size(), key);
			if (offset >= sst.size()) {
				return "";
			} else {
				return { sst[offset].second.data(), sst[offset].second.size() };
			}
		}
	private:
		friend class TableIterator;
		[[nodiscard]] uint64_t BinarySearchGet(const uint64_t& start, const uint64_t& end, const uint64_t& key) const;
		bool isFilter = true;
		Arena arena;
		std::vector<std::pair<uint64_t, Slice>> sst;
		Slice bloom;
		uint64_t timestamp = 0;
	public:
		class TableIterator {
		public:
			explicit TableIterator(const Table* table) : _table(table), _cur(0) {
				_end = _table->sst.size();
			};
			~TableIterator() {
				delete _table;
			}
			[[nodiscard]] bool hasNext() const {
				return _end > _cur;
			}
			const Slice value() const {
				return _table->sst[_cur].second;
			}
			const uint64_t& key() const {
				return _table->sst[_cur].first;
			}
			void next() {
				_cur++;
			}
			void seek(const uint64_t& key) {
				_cur = _table->BinarySearchGet(0, _end, key);
				_end = _table->sst.size();
			}

			void seek(const uint64_t& K1, const uint64_t& K2) {
				_cur = _table->BinarySearchGet(0, _end, K1);
				_end = _table->BinarySearchGet(0, _end, K1);
				if (_end != _table->sst.size()) _end++;
			}

			void seekToFirst() {
				_cur = 0;
			}

			uint64_t timestamp() {
				return _table->timestamp;
			}

		private:
			// not include _end!!!!
			uint64_t _cur;
			uint64_t _end;
			const Table* _table;
		};
		TableIterator* Iterator() {
			return new Table::TableIterator(this);
		}
	};



}

#endif //LSMKV_SRC_TABLE_H
