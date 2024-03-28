#ifndef FILENAME_H
#define FILENAME_H

#include <string>
#include <cstdint>
namespace LSMKV {
	static inline std::string MakeFileName(const std::string& dbname, uint64_t number, const std::string& suffix) {
		return dbname + "/" + std::to_string(number) + "." + suffix;
	}
	static inline std::string SSTFileName(const std::string& dbname, uint64_t number) {
		return MakeFileName(dbname, number, "sst");
	}
	static inline std::string VLogFileName(const std::string& dbname) {
		return dbname + "/" + ".vlog";
	}
	static inline std::string VersionFileName(const std::string& dbname) {
		return dbname + "/" + ".current";
	}
	static inline std::string LevelDirName(const std::string& dbname, uint64_t number) {
		return dbname+"/" + "level-" + std::to_string(number);
	}
}

#endif //FILENAME_H
