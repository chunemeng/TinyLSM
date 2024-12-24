cmake_minimum_required(VERSION 3.22.1)
project(LSMKV.handout)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_BUILD_TYPE Debug)


SET(CMAKE_CXX_FLAGS "-O0 -march=native -w -ftree-vectorize")

OPTION(ENABLE_TEST "Enable test" ON)
OPTION(ENABLE_BENCHMARK "Enable benchmark" OFF)
OPTION(ENABLE_DEBUG "Enable debug" ON)
OPTION(ENABLE_ASAN "Enable AddressSanitizer" OFF)
OPTION(ENABLE_UBSAN "Enable UndefinedBehaviorSanitizer" OFF)
OPTION(ENABLE_TSAN "Enable ThreadSanitizer" OFF)
OPTION(ENABLE_MSAN "Enable MemorySanitizer" OFF)


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

if (ENABLE_MSAN)
    message(STATUS "MemorySanitizer enabled")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fsanitize=memory")
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -fsanitize=memory")
endif ()

if (UNIX)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -pthread")
endif ()

#if (ENABLE_DEBUG)
#    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g")
#endif ()

if (ENABLE_BECHMARK)
    add_subdirectory("third_party/benchmark")
    add_executable(DBBench benchmark/dbbench.cpp test/testutil.cc test/testutil.h ${SRC_FILES})
    target_link_libraries(DBBench benchmark benchmark_main)
endif ()

add_subdirectory("third_party/googletest")

add_custom_target(clear
        COMMAND ${CMAKE_COMMAND} -E remove_directory ${CMAKE_BINARY_DIR}/data
        COMMENT "Cleaning up..."
)

set_target_properties(clear PROPERTIES
        ADDITIONAL_MAKE_CLEAN_FILES "${CMAKE_BINARY_DIR}/CMakeFiles"
)

set(SRC_FILES
        kvstore.cc
        kvstore.h
        test/performance.h
        kvstore_api.h
        MurmurHash3.h
        utils.h
        utils/arena.cpp
        src/include/skiplist.h
        src/include/memtable.h
        src/memtable.cpp
        utils/bloomfilter.h
        utils/iterator.h
        utils/slice.h
        utils/option.h
        src/builder.cpp
        src/include/builder.h
        utils/filemeta.h
        utils/coding.h
        utils/filename.h
        utils/file.h
        src/include/version.h
        src/include/vlogbuilder.h
        src/include/table.h
        src/cache.cpp
        src/include/cache.h
        src/vlogcache.cpp
        src/include/vlogcache.h
        src/keycache.cpp
        src/include/keycache.h
)

add_executable(TEST test/performance_test.cpp ${SRC_FILES})

add_executable(LSMKVC correctness.cc test.h ${SRC_FILES}
        src/include/scheduler.hpp
        src/include/queue.hpp
        src/include/thread.h
)

add_executable(LSMKVP persistence.cc test.h ${SRC_FILES}
        src/include/scheduler.hpp
        src/include/queue.hpp
        src/include/thread.h
)

add_executable(Put test/throughput_test.cpp ${SRC_FILES})

add_executable(Get test/random_get_test.cpp ${SRC_FILES})

add_executable(Bloom test/bloom_test.cc test/testutil.cc test/testutil.h ${SRC_FILES}
        test/same_timestamp_test.cpp)

target_link_libraries(Bloom gtest gmock gtest_main gmock_main)

