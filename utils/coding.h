#ifndef CODING_H
#define CODING_H

#include <cstdint>
inline void EncodeFixed32(char* dst, uint32_t value) {
	uint8_t* const buffer = reinterpret_cast<uint8_t*>(dst);

	buffer[0] = static_cast<uint8_t>(value);
	buffer[1] = static_cast<uint8_t>(value >> 8);
	buffer[2] = static_cast<uint8_t>(value >> 16);
	buffer[3] = static_cast<uint8_t>(value >> 24);
}

inline void EncodeFixed64(char* dst, uint64_t value) {
	uint8_t* const buffer = reinterpret_cast<uint8_t*>(dst);

	buffer[0] = static_cast<uint8_t>(value);
	buffer[1] = static_cast<uint8_t>(value >> 8);
	buffer[2] = static_cast<uint8_t>(value >> 16);
	buffer[3] = static_cast<uint8_t>(value >> 24);
	buffer[4] = static_cast<uint8_t>(value >> 32);
	buffer[5] = static_cast<uint8_t>(value >> 40);
	buffer[6] = static_cast<uint8_t>(value >> 48);
	buffer[7] = static_cast<uint8_t>(value >> 56);
}

#endif //CODING_H
