#pragma once

#include "coding.h"
#include "file.h"
#include <cstdio>
#include <cstring>
#include <dirent.h>
#include <fcntl.h>
#include <memory>
#include <sstream>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <vector>


#define PAGE_SIZE (4 * 1024)

namespace utils {
/**
     * Check whether directory exists
     * @param path directory to be checked.
     * @return ture if directory exists, false otherwise.
     */
static inline bool dirExists(const std::string &path) {
    struct stat st {};
    int ret = stat(path.c_str(), &st);
    return ret == 0 && st.st_mode & S_IFDIR;
}

/**
     * list all filename in a directory
     * @param path directory path.
     * @param ret all files name in directory.
     * @return files number.
     */
static inline int scanDir(const std::string &path, std::vector<std::string> &ret) {
    DIR *dir;
    struct dirent *rent;
    dir = opendir(path.c_str());
    if (dir == nullptr) {
        return -1;
    }
    char s[100];
    while ((rent = readdir(dir))) {
        strcpy(s, rent->d_name);
        if (s[0] != '.') {
            ret.emplace_back(s);
        }
    }
    closedir(dir);
    return ret.size();
}

/**
      * list all filename in a directory
      * @param path directory path.
     * @param ret all files name in directory.
      * @return files number.
      */
static inline int scanDirs(const std::string &path, std::vector<std::string> &ret) {
    DIR *dir;
    struct dirent *rent;
    dir = opendir(path.c_str());
    if (dir == nullptr) {
        return -1;
    }
    char s[100];
    while ((rent = readdir(dir))) {
        if (rent->d_type == DT_DIR) {
            strcpy(s, rent->d_name);
            if (s[0] != '.') {
                ret.emplace_back(s);
            }
        }
    }
    closedir(dir);
    return ret.size();
}

/**
     * Create directory
     * @param path directory to be created.
     * @return 0 if directory is created successfully, -1 otherwise.
     */
static inline int _mkdir(const std::string &path) {
    return ::mkdir(path.c_str(), 0775);
}

/**
     * Create directory recursively
     * @param path directory to be created.
     * @return 0 if directory is created successfully, -1 otherwise.
     */
static inline int mkdir(const std::string &path) {
    std::string currentPath = "";
    std::string dirName;
    std::stringstream ss(path);

    while (std::getline(ss, dirName, '/')) {
        currentPath += dirName;
        if (!dirExists(currentPath) && _mkdir(currentPath.c_str()) != 0) {
            return -1;
        }
        currentPath += "/";
    }
    return 0;
}

/**
     * Delete a empty directory
     * @param path directory to be deleted.
     * @return 0 if delete successfully, -1 otherwise.
     */
static inline int rmdir(const std::string &path) {
    return ::rmdir(path.c_str());
}

/**
     * Delete a file
     * @param path file to be deleted.
     * @return 0 if delete successfully, -1 otherwise.
     */
static inline int rmfile(const std::string &path) {
    return ::unlink(path.c_str());
}

/**
     * Reclaim space of a file
     * @param path file to be reclaimed.
     * @param offset offset from the beginning of the file you want to start reclaiming at.
     * @param len number of bytes you want to reclaim.
     * @return -1 if fail to open the file, -2 if fallocate fail, and 0 if reclaim space successfully
     * @attention logic _size of a file will NOT change after being reclaimed successfully,
     * so always make sure to calculate offset seems like the file has not been reclaimed.
     */
static inline int de_alloc_file(const std::string &path, off64_t offset, off64_t len) {
    int fd = open(path.c_str(), O_RDWR, 0444);
    if (fd < 0) {
        perror("open");
        return -1;
    }

    // If you want to call fallocate yourself,
    // the code below to handle offset and len is very IMPORTANT!
    // We strongly recommend you to call fallocate yourself only if you arena familiar with it!
    len += offset % PAGE_SIZE;
    offset = offset / PAGE_SIZE * PAGE_SIZE;

    if (fallocate64(fd, FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE, offset, len) < 0) {
        perror("fallocate");
        return -2;
    }

    close(fd);

    return 0;
}

/**
     * find the offset of the first block (_size of a blcok is 4kB) that has _data from the beginning of a file
     * @param path file to be searched for.
     * @return -1 if fail to open, offset if find successfully.
     */
static inline off64_t seek_data_block(const std::string &path) {
    int fd = open(path.c_str(), O_RDWR, 0644);
    if (fd < 0) {
        perror("open");
        return -1;
    }
    off64_t offset = lseek(fd, 0, SEEK_DATA);
    close(fd);
    return offset;
}

/**
     * util function used by crc16, you needn't call this function yourself
     */
#define POLYNOMIAL 0x1021

static inline std::unique_ptr<uint16_t[]> generate_crc16_table() {
    std::unique_ptr<uint16_t[]> table(new uint16_t[256]{0});
    for (int i = 0; i < 256; i++) {
        uint16_t value = 0;
        uint16_t temp = i << 8;
        for (int j = 0; j < 8; j++) {
            if ((value ^ temp) & 0x8000) {
                value = (value << 1) ^ POLYNOMIAL;
            } else {
                value <<= 1;
            }
            temp <<= 1;
        }
        table[i] = value;
    }
    return table;
}

constexpr static inline std::array<uint16_t, 256> generate_crc16_table_array() {
    std::array<uint16_t, 256> table{0};
    for (int i = 0; i < 256; i++) {
        uint16_t value = 0;
        uint16_t temp = i << 8;
        for (int j = 0; j < 8; j++) {
            if ((value ^ temp) & 0x8000) {
                value = (value << 1) ^ POLYNOMIAL;
            } else {
                value <<= 1;
            }
            temp <<= 1;
        }
        table[i] = value;
    }
    return table;
}

//    static const std::array<uint16_t,256> crc16_table = std::move(generate_crc16_table_array());
static constexpr uint16_t crc16_table[8][256] =
        {{0x0,
          0x1021, 0x2042, 0x3063, 0x4084, 0x50a5, 0x60c6, 0x70e7, 0x8108, 0x9129, 0xa14a, 0xb16b, 0xc18c, 0xd1ad, 0xe1ce, 0xf1ef, 0x1231,
          0x210, 0x3273, 0x2252, 0x52b5, 0x4294, 0x72f7, 0x62d6, 0x9339, 0x8318, 0xb37b, 0xa35a, 0xd3bd, 0xc39c, 0xf3ff, 0xe3de, 0x2462,
          0x3443, 0x420, 0x1401, 0x64e6, 0x74c7, 0x44a4, 0x5485, 0xa56a, 0xb54b, 0x8528, 0x9509, 0xe5ee, 0xf5cf, 0xc5ac, 0xd58d, 0x3653,
          0x2672, 0x1611, 0x630, 0x76d7, 0x66f6, 0x5695, 0x46b4, 0xb75b, 0xa77a, 0x9719, 0x8738, 0xf7df, 0xe7fe, 0xd79d, 0xc7bc, 0x48c4,
          0x58e5, 0x6886, 0x78a7, 0x840, 0x1861, 0x2802, 0x3823, 0xc9cc, 0xd9ed, 0xe98e, 0xf9af, 0x8948, 0x9969, 0xa90a, 0xb92b, 0x5af5,
          0x4ad4, 0x7ab7, 0x6a96, 0x1a71, 0xa50, 0x3a33, 0x2a12, 0xdbfd, 0xcbdc, 0xfbbf, 0xeb9e, 0x9b79, 0x8b58, 0xbb3b, 0xab1a, 0x6ca6,
          0x7c87, 0x4ce4, 0x5cc5, 0x2c22, 0x3c03, 0xc60, 0x1c41, 0xedae, 0xfd8f, 0xcdec, 0xddcd, 0xad2a, 0xbd0b, 0x8d68, 0x9d49, 0x7e97,
          0x6eb6, 0x5ed5, 0x4ef4, 0x3e13, 0x2e32, 0x1e51, 0xe70, 0xff9f, 0xefbe, 0xdfdd, 0xcffc, 0xbf1b, 0xaf3a, 0x9f59, 0x8f78, 0x9188,
          0x81a9, 0xb1ca, 0xa1eb, 0xd10c, 0xc12d, 0xf14e, 0xe16f, 0x1080, 0xa1, 0x30c2, 0x20e3, 0x5004, 0x4025, 0x7046, 0x6067, 0x83b9,
          0x9398, 0xa3fb, 0xb3da, 0xc33d, 0xd31c, 0xe37f, 0xf35e, 0x2b1, 0x1290, 0x22f3, 0x32d2, 0x4235, 0x5214, 0x6277, 0x7256, 0xb5ea,
          0xa5cb, 0x95a8, 0x8589, 0xf56e, 0xe54f, 0xd52c, 0xc50d, 0x34e2, 0x24c3, 0x14a0, 0x481, 0x7466, 0x6447, 0x5424, 0x4405, 0xa7db,
          0xb7fa, 0x8799, 0x97b8, 0xe75f, 0xf77e, 0xc71d, 0xd73c, 0x26d3, 0x36f2, 0x691, 0x16b0, 0x6657, 0x7676, 0x4615, 0x5634, 0xd94c,
          0xc96d, 0xf90e, 0xe92f, 0x99c8, 0x89e9, 0xb98a, 0xa9ab, 0x5844, 0x4865, 0x7806, 0x6827, 0x18c0, 0x8e1, 0x3882, 0x28a3, 0xcb7d,
          0xdb5c, 0xeb3f, 0xfb1e, 0x8bf9, 0x9bd8, 0xabbb, 0xbb9a, 0x4a75, 0x5a54, 0x6a37, 0x7a16, 0xaf1, 0x1ad0, 0x2ab3, 0x3a92, 0xfd2e,
          0xed0f, 0xdd6c, 0xcd4d, 0xbdaa, 0xad8b, 0x9de8, 0x8dc9, 0x7c26, 0x6c07, 0x5c64, 0x4c45, 0x3ca2, 0x2c83, 0x1ce0, 0xcc1, 0xef1f,
          0xff3e, 0xcf5d, 0xdf7c, 0xaf9b, 0xbfba, 0x8fd9, 0x9ff8, 0x6e17, 0x7e36, 0x4e55, 0x5e74, 0x2e93, 0x3eb2, 0xed1, 0x1ef0},
         {0x0,
          0x3331, 0x6662, 0x5553, 0xccc4, 0xfff5, 0xaaa6, 0x9997, 0x89a9, 0xba98, 0xefcb, 0xdcfa, 0x456d, 0x765c, 0x230f, 0x103e, 0x373,
          0x3042, 0x6511, 0x5620, 0xcfb7, 0xfc86, 0xa9d5, 0x9ae4, 0x8ada, 0xb9eb, 0xecb8, 0xdf89, 0x461e, 0x752f, 0x207c, 0x134d, 0x6e6,
          0x35d7, 0x6084, 0x53b5, 0xca22, 0xf913, 0xac40, 0x9f71, 0x8f4f, 0xbc7e, 0xe92d, 0xda1c, 0x438b, 0x70ba, 0x25e9, 0x16d8, 0x595,
          0x36a4, 0x63f7, 0x50c6, 0xc951, 0xfa60, 0xaf33, 0x9c02, 0x8c3c, 0xbf0d, 0xea5e, 0xd96f, 0x40f8, 0x73c9, 0x269a, 0x15ab, 0xdcc,
          0x3efd, 0x6bae, 0x589f, 0xc108, 0xf239, 0xa76a, 0x945b, 0x8465, 0xb754, 0xe207, 0xd136, 0x48a1, 0x7b90, 0x2ec3, 0x1df2, 0xebf,
          0x3d8e, 0x68dd, 0x5bec, 0xc27b, 0xf14a, 0xa419, 0x9728, 0x8716, 0xb427, 0xe174, 0xd245, 0x4bd2, 0x78e3, 0x2db0, 0x1e81, 0xb2a,
          0x381b, 0x6d48, 0x5e79, 0xc7ee, 0xf4df, 0xa18c, 0x92bd, 0x8283, 0xb1b2, 0xe4e1, 0xd7d0, 0x4e47, 0x7d76, 0x2825, 0x1b14, 0x859,
          0x3b68, 0x6e3b, 0x5d0a, 0xc49d, 0xf7ac, 0xa2ff, 0x91ce, 0x81f0, 0xb2c1, 0xe792, 0xd4a3, 0x4d34, 0x7e05, 0x2b56, 0x1867, 0x1b98,
          0x28a9, 0x7dfa, 0x4ecb, 0xd75c, 0xe46d, 0xb13e, 0x820f, 0x9231, 0xa100, 0xf453, 0xc762, 0x5ef5, 0x6dc4, 0x3897, 0xba6, 0x18eb,
          0x2bda, 0x7e89, 0x4db8, 0xd42f, 0xe71e, 0xb24d, 0x817c, 0x9142, 0xa273, 0xf720, 0xc411, 0x5d86, 0x6eb7, 0x3be4, 0x8d5, 0x1d7e,
          0x2e4f, 0x7b1c, 0x482d, 0xd1ba, 0xe28b, 0xb7d8, 0x84e9, 0x94d7, 0xa7e6, 0xf2b5, 0xc184, 0x5813, 0x6b22, 0x3e71, 0xd40, 0x1e0d,
          0x2d3c, 0x786f, 0x4b5e, 0xd2c9, 0xe1f8, 0xb4ab, 0x879a, 0x97a4, 0xa495, 0xf1c6, 0xc2f7, 0x5b60, 0x6851, 0x3d02, 0xe33, 0x1654,
          0x2565, 0x7036, 0x4307, 0xda90, 0xe9a1, 0xbcf2, 0x8fc3, 0x9ffd, 0xaccc, 0xf99f, 0xcaae, 0x5339, 0x6008, 0x355b, 0x66a, 0x1527,
          0x2616, 0x7345, 0x4074, 0xd9e3, 0xead2, 0xbf81, 0x8cb0, 0x9c8e, 0xafbf, 0xfaec, 0xc9dd, 0x504a, 0x637b, 0x3628, 0x519, 0x10b2,
          0x2383, 0x76d0, 0x45e1, 0xdc76, 0xef47, 0xba14, 0x8925, 0x991b, 0xaa2a, 0xff79, 0xcc48, 0x55df, 0x66ee, 0x33bd, 0x8c, 0x13c1,
          0x20f0, 0x75a3, 0x4692, 0xdf05, 0xec34, 0xb967, 0x8a56, 0x9a68, 0xa959, 0xfc0a, 0xcf3b, 0x56ac, 0x659d, 0x30ce, 0x3ff},
         {0x0,
          0x3730, 0x6e60, 0x5950, 0xdcc0, 0xebf0, 0xb2a0, 0x8590, 0xa9a1, 0x9e91, 0xc7c1, 0xf0f1, 0x7561, 0x4251, 0x1b01, 0x2c31, 0x4363,
          0x7453, 0x2d03, 0x1a33, 0x9fa3, 0xa893, 0xf1c3, 0xc6f3, 0xeac2, 0xddf2, 0x84a2, 0xb392, 0x3602, 0x132, 0x5862, 0x6f52, 0x86c6,
          0xb1f6, 0xe8a6, 0xdf96, 0x5a06, 0x6d36, 0x3466, 0x356, 0x2f67, 0x1857, 0x4107, 0x7637, 0xf3a7, 0xc497, 0x9dc7, 0xaaf7, 0xc5a5,
          0xf295, 0xabc5, 0x9cf5, 0x1965, 0x2e55, 0x7705, 0x4035, 0x6c04, 0x5b34, 0x264, 0x3554, 0xb0c4, 0x87f4, 0xdea4, 0xe994, 0x1dad,
          0x2a9d, 0x73cd, 0x44fd, 0xc16d, 0xf65d, 0xaf0d, 0x983d, 0xb40c, 0x833c, 0xda6c, 0xed5c, 0x68cc, 0x5ffc, 0x6ac, 0x319c, 0x5ece,
          0x69fe, 0x30ae, 0x79e, 0x820e, 0xb53e, 0xec6e, 0xdb5e, 0xf76f, 0xc05f, 0x990f, 0xae3f, 0x2baf, 0x1c9f, 0x45cf, 0x72ff, 0x9b6b,
          0xac5b, 0xf50b, 0xc23b, 0x47ab, 0x709b, 0x29cb, 0x1efb, 0x32ca, 0x5fa, 0x5caa, 0x6b9a, 0xee0a, 0xd93a, 0x806a, 0xb75a, 0xd808,
          0xef38, 0xb668, 0x8158, 0x4c8, 0x33f8, 0x6aa8, 0x5d98, 0x71a9, 0x4699, 0x1fc9, 0x28f9, 0xad69, 0x9a59, 0xc309, 0xf439, 0x3b5a,
          0xc6a, 0x553a, 0x620a, 0xe79a, 0xd0aa, 0x89fa, 0xbeca, 0x92fb, 0xa5cb, 0xfc9b, 0xcbab, 0x4e3b, 0x790b, 0x205b, 0x176b, 0x7839,
          0x4f09, 0x1659, 0x2169, 0xa4f9, 0x93c9, 0xca99, 0xfda9, 0xd198, 0xe6a8, 0xbff8, 0x88c8, 0xd58, 0x3a68, 0x6338, 0x5408, 0xbd9c,
          0x8aac, 0xd3fc, 0xe4cc, 0x615c, 0x566c, 0xf3c, 0x380c, 0x143d, 0x230d, 0x7a5d, 0x4d6d, 0xc8fd, 0xffcd, 0xa69d, 0x91ad, 0xfeff,
          0xc9cf, 0x909f, 0xa7af, 0x223f, 0x150f, 0x4c5f, 0x7b6f, 0x575e, 0x606e, 0x393e, 0xe0e, 0x8b9e, 0xbcae, 0xe5fe, 0xd2ce, 0x26f7,
          0x11c7, 0x4897, 0x7fa7, 0xfa37, 0xcd07, 0x9457, 0xa367, 0x8f56, 0xb866, 0xe136, 0xd606, 0x5396, 0x64a6, 0x3df6, 0xac6, 0x6594,
          0x52a4, 0xbf4, 0x3cc4, 0xb954, 0x8e64, 0xd734, 0xe004, 0xcc35, 0xfb05, 0xa255, 0x9565, 0x10f5, 0x27c5, 0x7e95, 0x49a5, 0xa031,
          0x9701, 0xce51, 0xf961, 0x7cf1, 0x4bc1, 0x1291, 0x25a1, 0x990, 0x3ea0, 0x67f0, 0x50c0, 0xd550, 0xe260, 0xbb30, 0x8c00, 0xe352,
          0xd462, 0x8d32, 0xba02, 0x3f92, 0x8a2, 0x51f2, 0x66c2, 0x4af3, 0x7dc3, 0x2493, 0x13a3, 0x9633, 0xa103, 0xf853, 0xcf63},
         {0x0,
          0x76b4, 0xed68, 0x9bdc, 0xcaf1, 0xbc45, 0x2799, 0x512d, 0x85c3, 0xf377, 0x68ab, 0x1e1f, 0x4f32, 0x3986, 0xa25a, 0xd4ee, 0x1ba7,
          0x6d13, 0xf6cf, 0x807b, 0xd156, 0xa7e2, 0x3c3e, 0x4a8a, 0x9e64, 0xe8d0, 0x730c, 0x5b8, 0x5495, 0x2221, 0xb9fd, 0xcf49, 0x374e,
          0x41fa, 0xda26, 0xac92, 0xfdbf, 0x8b0b, 0x10d7, 0x6663, 0xb28d, 0xc439, 0x5fe5, 0x2951, 0x787c, 0xec8, 0x9514, 0xe3a0, 0x2ce9,
          0x5a5d, 0xc181, 0xb735, 0xe618, 0x90ac, 0xb70, 0x7dc4, 0xa92a, 0xdf9e, 0x4442, 0x32f6, 0x63db, 0x156f, 0x8eb3, 0xf807, 0x6e9c,
          0x1828, 0x83f4, 0xf540, 0xa46d, 0xd2d9, 0x4905, 0x3fb1, 0xeb5f, 0x9deb, 0x637, 0x7083, 0x21ae, 0x571a, 0xccc6, 0xba72, 0x753b,
          0x38f, 0x9853, 0xeee7, 0xbfca, 0xc97e, 0x52a2, 0x2416, 0xf0f8, 0x864c, 0x1d90, 0x6b24, 0x3a09, 0x4cbd, 0xd761, 0xa1d5, 0x59d2,
          0x2f66, 0xb4ba, 0xc20e, 0x9323, 0xe597, 0x7e4b, 0x8ff, 0xdc11, 0xaaa5, 0x3179, 0x47cd, 0x16e0, 0x6054, 0xfb88, 0x8d3c, 0x4275,
          0x34c1, 0xaf1d, 0xd9a9, 0x8884, 0xfe30, 0x65ec, 0x1358, 0xc7b6, 0xb102, 0x2ade, 0x5c6a, 0xd47, 0x7bf3, 0xe02f, 0x969b, 0xdd38,
          0xab8c, 0x3050, 0x46e4, 0x17c9, 0x617d, 0xfaa1, 0x8c15, 0x58fb, 0x2e4f, 0xb593, 0xc327, 0x920a, 0xe4be, 0x7f62, 0x9d6, 0xc69f,
          0xb02b, 0x2bf7, 0x5d43, 0xc6e, 0x7ada, 0xe106, 0x97b2, 0x435c, 0x35e8, 0xae34, 0xd880, 0x89ad, 0xff19, 0x64c5, 0x1271, 0xea76,
          0x9cc2, 0x71e, 0x71aa, 0x2087, 0x5633, 0xcdef, 0xbb5b, 0x6fb5, 0x1901, 0x82dd, 0xf469, 0xa544, 0xd3f0, 0x482c, 0x3e98, 0xf1d1,
          0x8765, 0x1cb9, 0x6a0d, 0x3b20, 0x4d94, 0xd648, 0xa0fc, 0x7412, 0x2a6, 0x997a, 0xefce, 0xbee3, 0xc857, 0x538b, 0x253f, 0xb3a4,
          0xc510, 0x5ecc, 0x2878, 0x7955, 0xfe1, 0x943d, 0xe289, 0x3667, 0x40d3, 0xdb0f, 0xadbb, 0xfc96, 0x8a22, 0x11fe, 0x674a, 0xa803,
          0xdeb7, 0x456b, 0x33df, 0x62f2, 0x1446, 0x8f9a, 0xf92e, 0x2dc0, 0x5b74, 0xc0a8, 0xb61c, 0xe731, 0x9185, 0xa59, 0x7ced, 0x84ea,
          0xf25e, 0x6982, 0x1f36, 0x4e1b, 0x38af, 0xa373, 0xd5c7, 0x129, 0x779d, 0xec41, 0x9af5, 0xcbd8, 0xbd6c, 0x26b0, 0x5004, 0x9f4d,
          0xe9f9, 0x7225, 0x491, 0x55bc, 0x2308, 0xb8d4, 0xce60, 0x1a8e, 0x6c3a, 0xf7e6, 0x8152, 0xd07f, 0xa6cb, 0x3d17, 0x4ba3},
         {0x0,
          0xaa51, 0x4483, 0xeed2, 0x8906, 0x2357, 0xcd85, 0x67d4, 0x22d, 0xa87c, 0x46ae, 0xecff, 0x8b2b, 0x217a, 0xcfa8, 0x65f9, 0x45a,
          0xae0b, 0x40d9, 0xea88, 0x8d5c, 0x270d, 0xc9df, 0x638e, 0x677, 0xac26, 0x42f4, 0xe8a5, 0x8f71, 0x2520, 0xcbf2, 0x61a3, 0x8b4,
          0xa2e5, 0x4c37, 0xe666, 0x81b2, 0x2be3, 0xc531, 0x6f60, 0xa99, 0xa0c8, 0x4e1a, 0xe44b, 0x839f, 0x29ce, 0xc71c, 0x6d4d, 0xcee,
          0xa6bf, 0x486d, 0xe23c, 0x85e8, 0x2fb9, 0xc16b, 0x6b3a, 0xec3, 0xa492, 0x4a40, 0xe011, 0x87c5, 0x2d94, 0xc346, 0x6917, 0x1168,
          0xbb39, 0x55eb, 0xffba, 0x986e, 0x323f, 0xdced, 0x76bc, 0x1345, 0xb914, 0x57c6, 0xfd97, 0x9a43, 0x3012, 0xdec0, 0x7491, 0x1532,
          0xbf63, 0x51b1, 0xfbe0, 0x9c34, 0x3665, 0xd8b7, 0x72e6, 0x171f, 0xbd4e, 0x539c, 0xf9cd, 0x9e19, 0x3448, 0xda9a, 0x70cb, 0x19dc,
          0xb38d, 0x5d5f, 0xf70e, 0x90da, 0x3a8b, 0xd459, 0x7e08, 0x1bf1, 0xb1a0, 0x5f72, 0xf523, 0x92f7, 0x38a6, 0xd674, 0x7c25, 0x1d86,
          0xb7d7, 0x5905, 0xf354, 0x9480, 0x3ed1, 0xd003, 0x7a52, 0x1fab, 0xb5fa, 0x5b28, 0xf179, 0x96ad, 0x3cfc, 0xd22e, 0x787f, 0x22d0,
          0x8881, 0x6653, 0xcc02, 0xabd6, 0x187, 0xef55, 0x4504, 0x20fd, 0x8aac, 0x647e, 0xce2f, 0xa9fb, 0x3aa, 0xed78, 0x4729, 0x268a,
          0x8cdb, 0x6209, 0xc858, 0xaf8c, 0x5dd, 0xeb0f, 0x415e, 0x24a7, 0x8ef6, 0x6024, 0xca75, 0xada1, 0x7f0, 0xe922, 0x4373, 0x2a64,
          0x8035, 0x6ee7, 0xc4b6, 0xa362, 0x933, 0xe7e1, 0x4db0, 0x2849, 0x8218, 0x6cca, 0xc69b, 0xa14f, 0xb1e, 0xe5cc, 0x4f9d, 0x2e3e,
          0x846f, 0x6abd, 0xc0ec, 0xa738, 0xd69, 0xe3bb, 0x49ea, 0x2c13, 0x8642, 0x6890, 0xc2c1, 0xa515, 0xf44, 0xe196, 0x4bc7, 0x33b8,
          0x99e9, 0x773b, 0xdd6a, 0xbabe, 0x10ef, 0xfe3d, 0x546c, 0x3195, 0x9bc4, 0x7516, 0xdf47, 0xb893, 0x12c2, 0xfc10, 0x5641, 0x37e2,
          0x9db3, 0x7361, 0xd930, 0xbee4, 0x14b5, 0xfa67, 0x5036, 0x35cf, 0x9f9e, 0x714c, 0xdb1d, 0xbcc9, 0x1698, 0xf84a, 0x521b, 0x3b0c,
          0x915d, 0x7f8f, 0xd5de, 0xb20a, 0x185b, 0xf689, 0x5cd8, 0x3921, 0x9370, 0x7da2, 0xd7f3, 0xb027, 0x1a76, 0xf4a4, 0x5ef5, 0x3f56,
          0x9507, 0x7bd5, 0xd184, 0xb650, 0x1c01, 0xf2d3, 0x5882, 0x3d7b, 0x972a, 0x79f8, 0xd3a9, 0xb47d, 0x1e2c, 0xf0fe, 0x5aaf},
         {0x0,
          0x45a0, 0x8b40, 0xcee0, 0x6a1, 0x4301, 0x8de1, 0xc841, 0xd42, 0x48e2, 0x8602, 0xc3a2, 0xbe3, 0x4e43, 0x80a3, 0xc503, 0x1a84,
          0x5f24, 0x91c4, 0xd464, 0x1c25, 0x5985, 0x9765, 0xd2c5, 0x17c6, 0x5266, 0x9c86, 0xd926, 0x1167, 0x54c7, 0x9a27, 0xdf87, 0x3508,
          0x70a8, 0xbe48, 0xfbe8, 0x33a9, 0x7609, 0xb8e9, 0xfd49, 0x384a, 0x7dea, 0xb30a, 0xf6aa, 0x3eeb, 0x7b4b, 0xb5ab, 0xf00b, 0x2f8c,
          0x6a2c, 0xa4cc, 0xe16c, 0x292d, 0x6c8d, 0xa26d, 0xe7cd, 0x22ce, 0x676e, 0xa98e, 0xec2e, 0x246f, 0x61cf, 0xaf2f, 0xea8f, 0x6a10,
          0x2fb0, 0xe150, 0xa4f0, 0x6cb1, 0x2911, 0xe7f1, 0xa251, 0x6752, 0x22f2, 0xec12, 0xa9b2, 0x61f3, 0x2453, 0xeab3, 0xaf13, 0x7094,
          0x3534, 0xfbd4, 0xbe74, 0x7635, 0x3395, 0xfd75, 0xb8d5, 0x7dd6, 0x3876, 0xf696, 0xb336, 0x7b77, 0x3ed7, 0xf037, 0xb597, 0x5f18,
          0x1ab8, 0xd458, 0x91f8, 0x59b9, 0x1c19, 0xd2f9, 0x9759, 0x525a, 0x17fa, 0xd91a, 0x9cba, 0x54fb, 0x115b, 0xdfbb, 0x9a1b, 0x459c,
          0x3c, 0xcedc, 0x8b7c, 0x433d, 0x69d, 0xc87d, 0x8ddd, 0x48de, 0xd7e, 0xc39e, 0x863e, 0x4e7f, 0xbdf, 0xc53f, 0x809f, 0xd420,
          0x9180, 0x5f60, 0x1ac0, 0xd281, 0x9721, 0x59c1, 0x1c61, 0xd962, 0x9cc2, 0x5222, 0x1782, 0xdfc3, 0x9a63, 0x5483, 0x1123, 0xcea4,
          0x8b04, 0x45e4, 0x44, 0xc805, 0x8da5, 0x4345, 0x6e5, 0xc3e6, 0x8646, 0x48a6, 0xd06, 0xc547, 0x80e7, 0x4e07, 0xba7, 0xe128,
          0xa488, 0x6a68, 0x2fc8, 0xe789, 0xa229, 0x6cc9, 0x2969, 0xec6a, 0xa9ca, 0x672a, 0x228a, 0xeacb, 0xaf6b, 0x618b, 0x242b, 0xfbac,
          0xbe0c, 0x70ec, 0x354c, 0xfd0d, 0xb8ad, 0x764d, 0x33ed, 0xf6ee, 0xb34e, 0x7dae, 0x380e, 0xf04f, 0xb5ef, 0x7b0f, 0x3eaf, 0xbe30,
          0xfb90, 0x3570, 0x70d0, 0xb891, 0xfd31, 0x33d1, 0x7671, 0xb372, 0xf6d2, 0x3832, 0x7d92, 0xb5d3, 0xf073, 0x3e93, 0x7b33, 0xa4b4,
          0xe114, 0x2ff4, 0x6a54, 0xa215, 0xe7b5, 0x2955, 0x6cf5, 0xa9f6, 0xec56, 0x22b6, 0x6716, 0xaf57, 0xeaf7, 0x2417, 0x61b7, 0x8b38,
          0xce98, 0x78, 0x45d8, 0x8d99, 0xc839, 0x6d9, 0x4379, 0x867a, 0xc3da, 0xd3a, 0x489a, 0x80db, 0xc57b, 0xb9b, 0x4e3b, 0x91bc,
          0xd41c, 0x1afc, 0x5f5c, 0x971d, 0xd2bd, 0x1c5d, 0x59fd, 0x9cfe, 0xd95e, 0x17be, 0x521e, 0x9a5f, 0xdfff, 0x111f, 0x54bf},
         {0x0,
          0xb861, 0x60e3, 0xd882, 0xc1c6, 0x79a7, 0xa125, 0x1944, 0x93ad, 0x2bcc, 0xf34e, 0x4b2f, 0x526b, 0xea0a, 0x3288, 0x8ae9, 0x377b,
          0x8f1a, 0x5798, 0xeff9, 0xf6bd, 0x4edc, 0x965e, 0x2e3f, 0xa4d6, 0x1cb7, 0xc435, 0x7c54, 0x6510, 0xdd71, 0x5f3, 0xbd92, 0x6ef6,
          0xd697, 0xe15, 0xb674, 0xaf30, 0x1751, 0xcfd3, 0x77b2, 0xfd5b, 0x453a, 0x9db8, 0x25d9, 0x3c9d, 0x84fc, 0x5c7e, 0xe41f, 0x598d,
          0xe1ec, 0x396e, 0x810f, 0x984b, 0x202a, 0xf8a8, 0x40c9, 0xca20, 0x7241, 0xaac3, 0x12a2, 0xbe6, 0xb387, 0x6b05, 0xd364, 0xddec,
          0x658d, 0xbd0f, 0x56e, 0x1c2a, 0xa44b, 0x7cc9, 0xc4a8, 0x4e41, 0xf620, 0x2ea2, 0x96c3, 0x8f87, 0x37e6, 0xef64, 0x5705, 0xea97,
          0x52f6, 0x8a74, 0x3215, 0x2b51, 0x9330, 0x4bb2, 0xf3d3, 0x793a, 0xc15b, 0x19d9, 0xa1b8, 0xb8fc, 0x9d, 0xd81f, 0x607e, 0xb31a,
          0xb7b, 0xd3f9, 0x6b98, 0x72dc, 0xcabd, 0x123f, 0xaa5e, 0x20b7, 0x98d6, 0x4054, 0xf835, 0xe171, 0x5910, 0x8192, 0x39f3, 0x8461,
          0x3c00, 0xe482, 0x5ce3, 0x45a7, 0xfdc6, 0x2544, 0x9d25, 0x17cc, 0xafad, 0x772f, 0xcf4e, 0xd60a, 0x6e6b, 0xb6e9, 0xe88, 0xabf9,
          0x1398, 0xcb1a, 0x737b, 0x6a3f, 0xd25e, 0xadc, 0xb2bd, 0x3854, 0x8035, 0x58b7, 0xe0d6, 0xf992, 0x41f3, 0x9971, 0x2110, 0x9c82,
          0x24e3, 0xfc61, 0x4400, 0x5d44, 0xe525, 0x3da7, 0x85c6, 0xf2f, 0xb74e, 0x6fcc, 0xd7ad, 0xcee9, 0x7688, 0xae0a, 0x166b, 0xc50f,
          0x7d6e, 0xa5ec, 0x1d8d, 0x4c9, 0xbca8, 0x642a, 0xdc4b, 0x56a2, 0xeec3, 0x3641, 0x8e20, 0x9764, 0x2f05, 0xf787, 0x4fe6, 0xf274,
          0x4a15, 0x9297, 0x2af6, 0x33b2, 0x8bd3, 0x5351, 0xeb30, 0x61d9, 0xd9b8, 0x13a, 0xb95b, 0xa01f, 0x187e, 0xc0fc, 0x789d, 0x7615,
          0xce74, 0x16f6, 0xae97, 0xb7d3, 0xfb2, 0xd730, 0x6f51, 0xe5b8, 0x5dd9, 0x855b, 0x3d3a, 0x247e, 0x9c1f, 0x449d, 0xfcfc, 0x416e,
          0xf90f, 0x218d, 0x99ec, 0x80a8, 0x38c9, 0xe04b, 0x582a, 0xd2c3, 0x6aa2, 0xb220, 0xa41, 0x1305, 0xab64, 0x73e6, 0xcb87, 0x18e3,
          0xa082, 0x7800, 0xc061, 0xd925, 0x6144, 0xb9c6, 0x1a7, 0x8b4e, 0x332f, 0xebad, 0x53cc, 0x4a88, 0xf2e9, 0x2a6b, 0x920a, 0x2f98,
          0x97f9, 0x4f7b, 0xf71a, 0xee5e, 0x563f, 0x8ebd, 0x36dc, 0xbc35, 0x454, 0xdcd6, 0x64b7, 0x7df3, 0xc592, 0x1d10, 0xa571},
         {0x0,
          0x47d3, 0x8fa6, 0xc875, 0xf6d, 0x48be, 0x80cb, 0xc718, 0x1eda, 0x5909, 0x917c, 0xd6af, 0x11b7, 0x5664, 0x9e11, 0xd9c2, 0x3db4,
          0x7a67, 0xb212, 0xf5c1, 0x32d9, 0x750a, 0xbd7f, 0xfaac, 0x236e, 0x64bd, 0xacc8, 0xeb1b, 0x2c03, 0x6bd0, 0xa3a5, 0xe476, 0x7b68,
          0x3cbb, 0xf4ce, 0xb31d, 0x7405, 0x33d6, 0xfba3, 0xbc70, 0x65b2, 0x2261, 0xea14, 0xadc7, 0x6adf, 0x2d0c, 0xe579, 0xa2aa, 0x46dc,
          0x10f, 0xc97a, 0x8ea9, 0x49b1, 0xe62, 0xc617, 0x81c4, 0x5806, 0x1fd5, 0xd7a0, 0x9073, 0x576b, 0x10b8, 0xd8cd, 0x9f1e, 0xf6d0,
          0xb103, 0x7976, 0x3ea5, 0xf9bd, 0xbe6e, 0x761b, 0x31c8, 0xe80a, 0xafd9, 0x67ac, 0x207f, 0xe767, 0xa0b4, 0x68c1, 0x2f12, 0xcb64,
          0x8cb7, 0x44c2, 0x311, 0xc409, 0x83da, 0x4baf, 0xc7c, 0xd5be, 0x926d, 0x5a18, 0x1dcb, 0xdad3, 0x9d00, 0x5575, 0x12a6, 0x8db8,
          0xca6b, 0x21e, 0x45cd, 0x82d5, 0xc506, 0xd73, 0x4aa0, 0x9362, 0xd4b1, 0x1cc4, 0x5b17, 0x9c0f, 0xdbdc, 0x13a9, 0x547a, 0xb00c,
          0xf7df, 0x3faa, 0x7879, 0xbf61, 0xf8b2, 0x30c7, 0x7714, 0xaed6, 0xe905, 0x2170, 0x66a3, 0xa1bb, 0xe668, 0x2e1d, 0x69ce, 0xfd81,
          0xba52, 0x7227, 0x35f4, 0xf2ec, 0xb53f, 0x7d4a, 0x3a99, 0xe35b, 0xa488, 0x6cfd, 0x2b2e, 0xec36, 0xabe5, 0x6390, 0x2443, 0xc035,
          0x87e6, 0x4f93, 0x840, 0xcf58, 0x888b, 0x40fe, 0x72d, 0xdeef, 0x993c, 0x5149, 0x169a, 0xd182, 0x9651, 0x5e24, 0x19f7, 0x86e9,
          0xc13a, 0x94f, 0x4e9c, 0x8984, 0xce57, 0x622, 0x41f1, 0x9833, 0xdfe0, 0x1795, 0x5046, 0x975e, 0xd08d, 0x18f8, 0x5f2b, 0xbb5d,
          0xfc8e, 0x34fb, 0x7328, 0xb430, 0xf3e3, 0x3b96, 0x7c45, 0xa587, 0xe254, 0x2a21, 0x6df2, 0xaaea, 0xed39, 0x254c, 0x629f, 0xb51,
          0x4c82, 0x84f7, 0xc324, 0x43c, 0x43ef, 0x8b9a, 0xcc49, 0x158b, 0x5258, 0x9a2d, 0xddfe, 0x1ae6, 0x5d35, 0x9540, 0xd293, 0x36e5,
          0x7136, 0xb943, 0xfe90, 0x3988, 0x7e5b, 0xb62e, 0xf1fd, 0x283f, 0x6fec, 0xa799, 0xe04a, 0x2752, 0x6081, 0xa8f4, 0xef27, 0x7039,
          0x37ea, 0xff9f, 0xb84c, 0x7f54, 0x3887, 0xf0f2, 0xb721, 0x6ee3, 0x2930, 0xe145, 0xa696, 0x618e, 0x265d, 0xee28, 0xa9fb, 0x4d8d,
          0xa5e, 0xc22b, 0x85f8, 0x42e0, 0x533, 0xcd46, 0x8a95, 0x5357, 0x1484, 0xdcf1, 0x9b22, 0x5c3a, 0x1be9, 0xd39c, 0x944f}};

/**
     * generate crc16
     * @param data binary data used to generate crc16.
     * @return generated crc16.
     */
static inline uint16_t crc16(const std::vector<unsigned char> &data) {
    uint16_t crc = 0xFFFF;
    size_t i = 0, length = data.size();
    while (i < length) {
        crc = (crc << 8) ^ crc16_table[0][((crc >> 8) ^ data[i++]) & 0xFF];
    }
    return crc;
}

static inline uint16_t crc16_prefix(const char *prefix, size_t pre_len, const char *data, size_t len) {
    const auto *buf = reinterpret_cast<const uint8_t *>(data);
    uint16_t crc = 0xffff;
    for (int i = 0; i < pre_len; ++i) {
        crc = crc16_table[0][((crc >> 8) ^ *prefix++) & 0xff] ^ (crc << 8);
    }

    /* process individual bytes until we reach an 8-byte aligned pointer */
    while (len && ((uintptr_t) buf & 7) != 0) {
        crc = crc16_table[0][((crc >> 8) ^ *buf++) & 0xff] ^ (crc << 8);
        len--;
    }

    /* fast middle processing, 8 bytes (aligned!) per loop */
    /* clang-format off */
        while (len >= 8) {
            uint64_t n = *(uint64_t *) buf;
            crc = crc16_table[7][(n & 0xff) ^ ((crc >> 8) & 0xff)] ^
                  crc16_table[6][((n >> 8) & 0xff) ^ (crc & 0xff)] ^
                  crc16_table[5][(n >> 16) & 0xff] ^
                  crc16_table[4][(n >> 24) & 0xff] ^
                  crc16_table[3][(n >> 32) & 0xff] ^
                  crc16_table[2][(n >> 40) & 0xff] ^
                  crc16_table[1][(n >> 48) & 0xff] ^
                  crc16_table[0][n >> 56];
            buf += 8;
            len -= 8;
        }
    /* clang-format on */

    /* process remaining bytes (can't be larger than 8) */
    while (len) {
        crc = crc16_table[0][((crc >> 8) ^ *buf++) & 0xff] ^ (crc << 8);
        len--;
    }

    return crc;
}


/**
     * generate crc16
     * @param data binary data used to generate crc16.
     * @return generated crc16.
     */
static inline uint16_t crc16(const char *data, size_t len) {
    const auto *buf = reinterpret_cast<const uint8_t *>(data);
    uint16_t crc = 0xffff;
    /* process individual bytes until we reach an 8-byte aligned pointer */
    while (len && ((uintptr_t) buf & 7) != 0) {
        crc = crc16_table[0][((crc >> 8) ^ *buf++) & 0xff] ^ (crc << 8);
        len--;
    }

    /* fast middle processing, 8 bytes (aligned!) per loop */
    /* clang-format off */
        while (len >= 8) {
            uint64_t n = *(uint64_t *) buf;
            crc = crc16_table[7][(n & 0xff) ^ ((crc >> 8) & 0xff)] ^
                  crc16_table[6][((n >> 8) & 0xff) ^ (crc & 0xff)] ^
                  crc16_table[5][(n >> 16) & 0xff] ^
                  crc16_table[4][(n >> 24) & 0xff] ^
                  crc16_table[3][(n >> 32) & 0xff] ^
                  crc16_table[2][(n >> 40) & 0xff] ^
                  crc16_table[1][(n >> 48) & 0xff] ^
                  crc16_table[0][n >> 56];
            buf += 8;
            len -= 8;
        }
    /* clang-format on */

    /* process remaining bytes (can't be larger than 8) */
    while (len) {
        crc = crc16_table[0][((crc >> 8) ^ *buf++) & 0xff] ^ (crc << 8);
        len--;
    }

    return crc;
}


static inline uint64_t offset_tail(const std::string &path, uint64_t head) {
    LSMKV::SequentialFile *file;


    int fd = ::open(path.c_str(), O_RDONLY | LSMKV::kOpenBaseFlags);

    off64_t offset = ::lseek(fd, 0, SEEK_DATA);

    if (offset <= 0) {
        ::close(fd);
        return 0;
    }

    file = new LSMKV::SequentialFile(path, fd);


    LSMKV::FileGuard<LSMKV::SequentialFile> guard(file);


    // Can get length when read
    LSMKV::Slice result;
    const char magic = '\377';

    char tmp[PAGE_SIZE];

    uint32_t vlen;
    uint32_t cur_offset;
    uint64_t size;
    std::string buf;
    // waring!! bug in here
    while ((size = head - offset)) {
        cur_offset = 0;
        if (buf.size() >= PAGE_SIZE) {
            memcpy(tmp, buf.c_str(), PAGE_SIZE);
            buf = buf.substr(PAGE_SIZE);
        } else {
            memcpy(tmp, buf.c_str(), buf.size());
            auto read_size = PAGE_SIZE - buf.size();
            file->Read(read_size, &result, tmp + buf.size());
            buf.resize(0);
        }

        std::string_view view(result.data(), result.size());
        size_t magic_offset;
        while ((magic_offset = view.find(magic)) != std::string::npos) {
            cur_offset += magic_offset;
            view.remove_prefix(magic_offset);
            // todo: if cant read the vlen
            if (view.size() < 15) [[unlikely]] {
                auto buf_size = PAGE_SIZE;
                if (buf.size() < buf_size) {
                    if (buf.capacity() < buf_size) {
                        buf.reserve(buf_size);
                    }
                    auto old_size = buf.size();
                    buf.resize(buf_size);

                    file->Read(buf_size - old_size, &result, buf.data() + old_size);
                }
                int i = 0;
                uint32_t real_len{};
                char *len = reinterpret_cast<char *>(&vlen);

                for (size_t s = 11; i < view.size(); i++) {
                    len[i++] = view[s];
                }

                assert(i <= 3);

                for (int j = 0; i < 4; ++i, ++j) {
                    len[i] = buf[i];
                }
                vlen = LSMKV::DecodeFixed32(len);

                buf_size = vlen + 15 - view.size();
                if (buf.size() < buf_size) {
                    if (buf.capacity() < buf_size) {
                        buf.reserve(buf_size);
                    }
                    auto old_size = buf.size();
                    buf.resize(buf_size);

                    file->Read(buf_size - old_size, &result, buf.data() + old_size);
                }
                uint16_t crc;
                if (view.size() >= 3) {
                    crc = LSMKV::DecodeFixed16(view.data() + 1);
                    if (crc16_prefix(view.data() + 3, view.size() - 3, buf.c_str(), buf_size) ==
                        crc) {
                        return offset + cur_offset;
                    }
                } else if (view.size() < 2) {
                    crc = LSMKV::DecodeFixed16(buf.c_str());
                    if (crc16(buf.c_str() + 2, vlen + 12) ==
                        crc) {
                        return offset + cur_offset;
                    }
                } else {
                    len[0] = view[1];
                    len[1] = buf[0];
                    crc = LSMKV::DecodeFixed16(len);
                    if (crc16(buf.c_str() + 1, vlen + 12) ==
                        crc) {
                        return offset + cur_offset;
                    }
                }
            } else {
                vlen = LSMKV::DecodeFixed32(view.data() + 11);

                if (offset + cur_offset + vlen + 15 > head) {
                    cur_offset++;
                    view.remove_prefix(1);
                    continue;
                }
                if (vlen + 15 > view.size()) [[unlikely]] {
                    auto buf_size = vlen + 15 - view.size();
                    if (buf.size() < buf_size) {
                        if (buf.capacity() < buf_size) {
                            buf.reserve(buf_size);
                        }
                        auto old_size = buf.size();
                        buf.resize(buf_size);

                        file->Read(buf_size - old_size, &result, buf.data() + old_size);
                    }
                    if (crc16_prefix(view.data() + 3, view.size() - 3, buf.c_str(), buf_size) ==
                        LSMKV::DecodeFixed16(view.data() + 1)) {
                        return offset + cur_offset;
                    }

                } else {
                    if (crc16(view.data() + 3, vlen + 12) ==
                        LSMKV::DecodeFixed16(view.data() + 1)) {
                        return offset + cur_offset;
                    }
                }
            }
            cur_offset++;
            view.remove_prefix(1);
        }
        offset += PAGE_SIZE;
    }

    if (size >= 15) {
        file->Read(size, &result, tmp);
        std::string_view view(result.data(), size);
        auto magic_offset = view.find(magic);
        while ((magic_offset = view.find(magic)) != std::string::npos) {
            cur_offset += magic_offset;
            if (view.size() < magic_offset + 15) {
                break;
            }

            vlen = LSMKV::DecodeFixed32(view.data() + magic_offset + 11);

            if (offset + cur_offset + vlen + 15 > head) {
                cur_offset++;
                view.remove_prefix(magic_offset + 1);
                continue;
            }
            if (vlen + 15 > view.size()) [[unlikely]] {
                break;
            } else {
                if (crc16(view.data() + 15 + magic_offset, vlen + 12) == LSMKV::DecodeFixed16(view.data() + 1)) {
                    return offset + cur_offset;
                }
            }
            cur_offset++;
            view.remove_prefix(magic_offset + 1);
        }
    }
    return head;
}

static inline bool rmfiles(std::string &path) {
    DIR *dir = opendir(path.c_str());
    if (dir == nullptr) {
        return false;
    }

    dirent *entry;
    while ((entry = readdir(dir)) != nullptr) {
        if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0) {
            continue;
        }

        std::string fullPath = std::string(path) + "/" + entry->d_name;

        if (entry->d_type == DT_DIR) {
            rmfiles(fullPath);
        } else {
            rmfile(fullPath);
        }
    }

    closedir(dir);

    return false;
}

static inline int mvfile(std::string &&old_path, std::string &&new_path) {
    return std::rename(old_path.c_str(), new_path.c_str());
}
}// namespace utils
