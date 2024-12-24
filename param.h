#pragma once
#include <string>


namespace LSMKV {

#ifdef LSM_DEFAULT_DATA_DIR
extern constexpr const char *kDefaultDataDir = LSM_DEFAULT_DATA_DIR;
#else
#define LSM_DEFAULT_DATA_DIR "./data"
extern constexpr const char *kDefaultDataDir = "./data";
#endif
#define VLOG_DIR "vlog"
#define LSM_DEFAULT_DATA_DIR_VLOG_DIR LSM_DEFAULT_DATA_DIR "/" VLOG_DIR

extern constexpr const char *kDefaultVLogDir = LSM_DEFAULT_DATA_DIR_VLOG_DIR;

}// namespace LSMKV