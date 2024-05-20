#ifndef VERSION_H
#define VERSION_H

#include <string>
#include <cstdint>
#include <vector>
#include <set>
#include "../../utils/file.h"
#include "../../utils/coding.h"
#include "../../utils/filename.h"
#include "../../utils.h"
namespace LSMKV {
	struct Version {
		explicit Version(const std::string& dbname) : filename(VersionFileName(dbname)) {
			// read from current file
			if (FileExists(filename)) {
				RandomReadableFile* file;
				NewRandomReadableFile(filename, &file);
				Slice slice;
				char buf[40];
				file->Read(0, 40, &slice, buf);
				if (slice.size() == 40) {
					fileno = DecodeFixed64(buf);
					timestamp = DecodeFixed64(buf + 8);
					head = DecodeFixed64(buf + 16);
					tail = DecodeFixed64(buf + 24);
					max_level = DecodeFixed64(buf + 32);
				}
                delete file;
			} else {
				WriteToFile(this);
			}
            EmplaceStatus();
		}

		~Version() {
			// write into current file
			WriteToFile(this);
		}

		static inline void WriteToFile(const Version *v) {
			WritableNoBufFile* file;
			NewWritableNoBufFile(v->filename, &file);
			char buf[40];
			EncodeFixed64(buf, v->fileno);
			EncodeFixed64(buf + 8, v->timestamp);
			EncodeFixed64(buf + 16, v->head);
			EncodeFixed64(buf + 24, v->tail);
			EncodeFixed64(buf + 32, v->max_level);
			file->WriteUnbuffered(buf, 40);
			delete file;
		}

        void EmplaceStatus() {
            // IF ONLY ADD TWO NEW LEVEL WILL NOT REVERSE
            max_level = max_level % 1000;
            status.reserve(max_level + 3);
            for (uint64_t i = 0; i <= max_level; i++) {
                // TODO MAYBE OVERFLOW (?)
                status.emplace_back();
            }
        }

		void reset() {
			fileno = 0;
			timestamp = 0;
			head = 0;
			tail = 0;
			max_level = 7;
            status.clear();
			WriteToFile(this);
            EmplaceStatus();
		}
		std::string DBName() const {
			return Dirname(filename);
		}

		void LoadStatus(uint64_t level, uint64_t file_no) {
			status[level].insert(file_no);
		}

		void ClearLevelStatus(uint64_t level, std::vector<uint64_t> old_files[2]) {
			std::string dir_name = DBDirName(filename);
			for (int i = 0; i < 2; ++i) {
				for (auto& it : old_files[i]) {
					status[level + i].erase(it);
					auto flag = utils::rmfile(SSTFilePath(dir_name, level + i, it));
                    assert(!flag);
                }
			}
		}
		// they are moved
		void MoveLevelStatus(uint64_t level, std::vector<uint64_t>& old_files) {
			for (auto& it : old_files) {
				status[level].erase(it);
				status[level + 1].insert(it);
			}
		}

		uint64_t LevelSize(uint64_t level) const {
			return status[level].size();
		}

		bool LevelOver(uint64_t level) {
			return status[level].size() > (1 << (level + 1));
		}
		void AddNewLevelStatus(uint64_t level, uint64_t file_no, uint64_t size) {
			for (uint64_t i = 0; i < size; i++) {
				status[level].insert(file_no + i);
			}
		}

		// Creat a New level
		void AddNewLevel() {
			status.emplace_back();
			max_level++;
            std::string dir_name = LevelDirName(DBName(), max_level);
            if (!utils::dirExists(dir_name)) {
                utils::_mkdir(dir_name);
            }
		}

		void ToNextLevel(uint64_t level, std::vector<uint64_t>& new_files) {
			for (auto new_file : new_files) {
				status[level + 1].insert(new_file);
			}
		}

		bool NeedNewLevel(uint64_t level) {
			assert(level <= max_level);
			return max_level == level;
		}

		std::set<uint64_t>& GetLevelStatus(uint64_t level) {
			assert(level < status.size());
			return status[level];
		}

		std::string filename;
		uint64_t fileno = 0;
		uint64_t timestamp = 1;
		uint64_t head = 0;
		uint64_t tail = 0;
		uint64_t max_level = 7;
        std::vector<std::set<uint64_t>> status;

    };
}

#endif //VERSION_H
