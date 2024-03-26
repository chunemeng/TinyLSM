#ifndef FILENAME_H
#define FILENAME_H

#include <string>
#include <cstdint>
namespace LSMKV {
	std::string MakeFileName(const std::string& dbname, uint64_t number, const std::string& suffix) {
		return dbname + "/" + std::to_string(number) + "." + suffix;
	}
	std::string SSTFileName(const std::string& dbname, uint64_t number) {
		return MakeFileName(dbname, number, "sst");
	}
	std::string VLogFileName(const std::string& dbname) {
		return dbname + "/" + ".vlog";
	}
	std::string VersionFileName(const std::string& dbname) {
		return dbname + "/" + ".current";
	}
}

#endif //FILENAME_H
