cmake_minimum_required(VERSION 3.22.1)
project(LSMKV)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_BUILD_TYPE Debug)


OPTION(ENABLE_TEST "Enable test" ON)
OPTION(ENABLE_BENCHMARK "Enable benchmark" ON)
OPTION(ENABLE_DEBUG "Enable debug" ON)
# Sanitizers(enable once at a time)
OPTION(ENABLE_ASAN "Enable AddressSanitizer" OFF)
OPTION(ENABLE_UBSAN "Enable UndefinedBehaviorSanitizer" OFF)
OPTION(ENABLE_TSAN "Enable ThreadSanitizer" OFF)
OPTION(LOCAL_DATA_DIR "Use local data directory" ON)


if (ENABLE_ASAN)
    message(STATUS "AddressSanitizer enabled")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=address -fno-omit-frame-pointer")
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fsanitize=address")
endif ()

if (ENABLE_UBSAN)
    message(STATUS "UndefinedBehaviorSanitizer enabled")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=undefined")
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fsanitize=undefined")
endif ()

if (ENABLE_TSAN)
    message(STATUS "ThreadSanitizer enabled")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=thread")
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fsanitize=thread")
endif ()

if (UNIX)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -pthread")
endif ()

SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -march=native -w -ftree-vectorize")

if (ENABLE_DEBUG)
    message(STATUS "Debug mode enabled")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g")
    SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O0 -march=native -w -ftree-vectorize")
else ()
    message(STATUS "Release mode enabled")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O2")
endif ()

if (ENABLE_BENCHMARK)
    add_subdirectory("third_party/benchmark")
    add_subdirectory(bench)
endif ()

if (ENABLE_TEST)
    add_subdirectory("third_party/googletest")
    add_subdirectory(test)
endif ()


add_custom_target(clear
        COMMAND ${CMAKE_COMMAND} -E remove_directory ${CMAKE_BINARY_DIR}/data
        COMMENT "Cleaning up..."
)

set_target_properties(clear PROPERTIES
        ADDITIONAL_MAKE_CLEAN_FILES "${CMAKE_BINARY_DIR}/CMakeFiles"
)



include_directories("${CMAKE_SOURCE_DIR}/include")

add_library(LSMKV STATIC kvstore.cc)

if (LOCAL_DATA_DIR)
    target_compile_definitions(LSMKV PRIVATE LSM_DEFAULT_DATA_DIR="${CMAKE_SOURCE_DIR}/data")
endif ()

add_subdirectory(src)






