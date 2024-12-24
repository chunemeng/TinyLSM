#ifndef CODING_H
#define CODING_H

#include <bit>
#include <cstdint>
#include <cstring>

namespace LSMKV {
inline uint64_t DecodeFixed64(const char *ptr) {
    uint64_t value;
    memcpy(&value, ptr, sizeof(uint64_t));
    if constexpr (std::endian::native == std::endian::big) {
#if defined(__GNUC__) || defined(__clang__)
        value = __builtin_bswap64(value);
#else
        auto *const buffer = reinterpret_cast<uint8_t *>(dst);
        return (static_cast<uint64_t>(buffer[0])) |
               (static_cast<uint64_t>(buffer[1]) << 8) |
               (static_cast<uint64_t>(buffer[2]) << 16) |
               (static_cast<uint64_t>(buffer[3]) << 24) |
               (static_cast<uint64_t>(buffer[4]) << 32) |
               (static_cast<uint64_t>(buffer[5]) << 40) |
               (static_cast<uint64_t>(buffer[6]) << 48) |
               (static_cast<uint64_t>(buffer[7]) << 56);
#endif
    }
    return value;
}

inline uint32_t DecodeFixed32(const char *ptr) {
    uint32_t value;
    memcpy(&value, ptr, sizeof(uint32_t));
    if constexpr (std::endian::native == std::endian::big) {
#if defined(__GNUC__) || defined(__clang__)
        value = __builtin_bswap32(value);
#else
        auto *const buffer = reinterpret_cast<uint8_t *>(dst);

        return (static_cast<uint32_t>(buffer[0])) |
               (static_cast<uint32_t>(buffer[1]) << 8) |
               (static_cast<uint32_t>(buffer[2]) << 16) |
               (static_cast<uint32_t>(buffer[3]) << 24);
#endif
    }
    return value;
}

inline uint16_t DecodeFixed16(const char *ptr) {
    uint16_t value;
    memcpy(&value, ptr, sizeof(uint16_t));
    if constexpr (std::endian::native == std::endian::big) {
#if defined(__GNUC__) || defined(__clang__)
        value = __builtin_bswap16(value);
#else
        auto *const buffer = reinterpret_cast<uint8_t *>(dst);
        return (static_cast<uint32_t>(buffer[0])) |
               (static_cast<uint32_t>(buffer[1]) << 8);
#endif
    }
    return value;
}

inline void EncodeFixed16(char *dst, uint16_t value) {
    auto *const buffer = reinterpret_cast<uint8_t *>(dst);
    if constexpr (std::endian::native == std::endian::big) {
#if defined(__GNUC__) || defined(__clang__)
        value = __builtin_bswap16(value);
#else
        buffer[0] = static_cast<uint8_t>(value);
        buffer[1] = static_cast<uint8_t>(value >> 8);
        buffer[2] = static_cast<uint8_t>(value >> 16);
        buffer[3] = static_cast<uint8_t>(value >> 24);
        return;
#endif
    }
    memcpy(buffer, &value, sizeof(uint16_t));
}

inline void EncodeFixed32(char *dst, uint32_t value) {
    auto *const buffer = reinterpret_cast<uint8_t *>(dst);
    if constexpr (std::endian::native == std::endian::big) {
#if defined(__GNUC__) || defined(__clang__)
        value = __builtin_bswap32(value);
#else
        buffer[0] = static_cast<uint8_t>(value);
        buffer[1] = static_cast<uint8_t>(value >> 8);
        buffer[2] = static_cast<uint8_t>(value >> 16);
        buffer[3] = static_cast<uint8_t>(value >> 24);
        return;
#endif
    }
    memcpy(buffer, &value, sizeof(uint32_t));
}

inline void EncodeFixed64(char *dst, uint64_t value) {

    auto *const buffer = reinterpret_cast<uint8_t *>(dst);
    if constexpr (std::endian::native == std::endian::big) {
#if defined(__GNUC__) || defined(__clang__)
        value = __builtin_bswap64(value);
#else
        buffer[0] = static_cast<uint8_t>(value);
        buffer[1] = static_cast<uint8_t>(value >> 8);
        buffer[2] = static_cast<uint8_t>(value >> 16);
        buffer[3] = static_cast<uint8_t>(value >> 24);
        buffer[4] = static_cast<uint8_t>(value >> 32);
        buffer[5] = static_cast<uint8_t>(value >> 40);
        buffer[6] = static_cast<uint8_t>(value >> 48);
        buffer[7] = static_cast<uint8_t>(value >> 56);
        return;
#endif
    }
    memcpy(buffer, &value, sizeof(uint64_t));
}


inline void memcpy_tiny(void *dst, const void *src, size_t len) {
    memcpy(dst, src, len);
}

}// namespace LSMKV

#endif//CODING_H
