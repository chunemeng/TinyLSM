cmake_minimum_required(VERSION 3.22.1)
project(LSMKV.handout)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_BUILD_TYPE Debug)

#SET(CMAKE_CXX_FLAGS "-O1 -march=native -w -ftree-vectorize -fstrict-aliasing   -fthread-jumps -falign-functions  -falign-jumps -falign-loops  -falign-labels -fcaller-saves -fcrossjumping -fcse-follow-jumps  -fcse-skip-blocks  -fdelete-null-pointer-checks -fdevirtualize -fdevirtualize-speculatively -fexpensive-optimizations -fgcse  -fgcse-lm -fhoist-adjacent-loads -finline-small-functions -findirect-inlining")
SET(CMAKE_CXX_FLAGS "-O2 -march=native -w -ftree-vectorize")

        #include(FetchContent)
#FetchContent_Declare(
#        googletest
#        URL https://github.com/google/googletest/archive/03597a01ee50ed33e9dfd640b249b4be3799d395.zip
#)
## For Windows: Prevent overriding the parent project's compiler/linker settings
#set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
#FetchContent_MakeAvailable(googletest)
#add_executable(SRC_TEST src/skiplist.h)
#target_link_libraries(
#        SRC_TEST
#        GTest::gtest_main
#)
#
#include(GoogleTest)
#gtest_discover_tests(SRC_TEST)

add_subdirectory("third_party/googletest")
add_subdirectory("third_party/benchmark")



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
add_executable(LSMKVP persistence.cc test.h ${SRC_FILES})

add_executable(Put test/throughput_test.cpp ${SRC_FILES})

add_executable(Get test/random_get_test.cpp ${SRC_FILES})

add_executable(Bloom test/bloom_test.cc test/testutil.cc test/testutil.h ${SRC_FILES}
        test/same_timestamp_test.cpp)

add_executable(DBBench benchmark/dbbench.cpp test/testutil.cc test/testutil.h ${SRC_FILES})

target_link_libraries(Bloom gtest gmock gtest_main gmock_main)

target_link_libraries(DBBench benchmark benchmark_main)
