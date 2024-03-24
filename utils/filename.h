#ifndef FILENAME_H
#define FILENAME_H

#include <string>
#include <cstdint>
std::string MakeFileName(const std::string& dbname, uint64_t number, const std::string& suffix) {
	return dbname + "/" + std::to_string(number) + "." + suffix;
}
std::string SSTFileName(const std::string& dbname, uint64_t number) {
	return MakeFileName(dbname, number, "sst");
}

#endif //FILENAME_H
