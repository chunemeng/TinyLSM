#ifndef VERSION_H
#define VERSION_H

#include <string>
#include <cstdint>
struct Version {
	Version(const std::string& file) {

	}
	uint64_t fileno = 0;
	uint64_t head = 0;
	uint64_t timestamp = 0;
};

#endif //VERSION_H
