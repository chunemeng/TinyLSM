#ifndef VERSION_H
#define VERSION_H

#include <string>
#include <cstdint>
#include <vector>
#include <set>
#include "../utils/file.h"
#include "../utils/coding.h"
#include "../utils/filename.h"
#include "../utils.h"
namespace LSMKV {
	struct Version {
		explicit Version(const std::string& dbname) : filename(VersionFileName(dbname)) {
			// read from current file
			if (FileExists(filename)) {
				RandomReadableFile* file;
				NewRandomReadableFile(filename, &file);
				Slice slice;
				char buf[32];
				file->Read(0, 32, &slice, buf);
				if (slice.size() == 32) {
					fileno = DecodeFixed64(buf);
					timestamp = DecodeFixed64(buf + 8);
					head = DecodeFixed64(buf + 16);
					tail = DecodeFixed64(buf + 24);
					max_level = DecodeFixed64(buf + 32);
				}
			} else {
				WriteToFile();
			}
			// IF ONLY ADD TWO NEW LEVEL WILL NOT REVERSE
			status.reserve(max_level + 3);
			for (uint64_t i = 0; i <= max_level; i++) {
				// TODO MAYBE OVERFLOW (?)
				status.emplace_back();
			}
		}

		~Version() {
			// write into current file
			WriteToFile();
		}

		inline void WriteToFile() const {
			WritableFile* file;
			NewWritableFile(filename, &file);
			char buf[40];
			EncodeFixed64(buf, fileno);
			EncodeFixed64(buf + 8, timestamp);
			EncodeFixed64(buf + 16, head);
			EncodeFixed64(buf + 24, tail);
			EncodeFixed64(buf + 32, max_level);
			file->Append(Slice(buf, 32));
			file->Close();
			delete file;
		}

		void reset() {
			fileno = 0;
			timestamp = 0;
			head = 0;
			tail = 0;
			max_level = 7;
			WriteToFile();
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
					status[level].erase(fileno);
					utils::rmfile(SSTFilePath(dir_name, level + i, it));
				}
			}
		}
		// they are moved
		void MoveLevelStatus(uint64_t level, std::vector<uint64_t>& old_files) {
			std::string dir_name = DBDirName(filename);
			for (auto& it : old_files) {
				status[level].erase(fileno);
				status[level + 1].insert(fileno);
			}
		}

		uint64_t LevelSize(uint64_t level) const {
			return status[level].size();
		}

		bool LevelOver(uint64_t level) {
			return status[level].size() >= (1 << (level + 1));
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

		std::vector<std::set<uint64_t>> status;
		std::string filename;
		uint64_t fileno = 0;
		uint64_t timestamp = 1;
		uint64_t head = 0;
		uint64_t tail = 0;
		uint64_t max_level = 7;
	};
}

#endif //VERSION_H
