#ifndef FILENAME_H
#define FILENAME_H

#include <string>
#include <cstdint>
namespace LSMKV {
	static inline std::string MakeFileName(const std::string& dbname, uint64_t number, const std::string& suffix) {
		return dbname + "/" + std::to_string(number) + "." + suffix;
	}
	static inline std::string SSTFileName(const std::string& level_name, uint64_t number) {
		return MakeFileName(level_name, number, "sst");
	}
	static inline std::string VLogFileName(const std::string& dbname) {
		return dbname + "/" + "vlog";
	}
	static inline std::string VersionFileName(const std::string& dbname) {
		return dbname + "/" + ".current";
	}
	static inline std::string LevelDirName(const std::string& dbname, uint64_t number) {
		return dbname+"/" + "level-" + std::to_string(number);
	}

    static inline std::string SSTFilePath(const std::string& level_dir, uint64_t number) {
        return SSTFileName(level_dir,number);
    }

	static inline std::string SSTFilePath(const std::string& dbname, uint64_t level, uint64_t number) {
		return SSTFileName(LevelDirName(dbname,level),number);
	}

	static inline std::string DBDirName(const std::string& filename) {
		std::string::size_type separator_pos = filename.rfind('/');
		if (separator_pos == std::string::npos) {
			return { "." };
		}

		assert(filename.find('/', separator_pos + 1) == std::string::npos);

		return filename.substr(0, separator_pos);
	}
//	static inline std::string JoinName(const std::string& dbname,...) {
//		return dbname+"/" + "level-" + std::to_string(number);
//	}
}

#endif //FILENAME_H
