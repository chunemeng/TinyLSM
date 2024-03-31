cmake_minimum_required(VERSION 3.22.1)
project(LSMKV.handout)

set(CMAKE_CXX_STANDARD 20)

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
add_executable(TEST lsm_test.cpp
        kvstore.cc
        kvstore.h
        kvstore_api.h
        MurmurHash3.h
        #        persistence.cc
        test.h
        utils.h
        utils/arena.cpp
        src/skiplist.h
        src/memtable.h
        src/memtable.cpp
        src/memtable.cpp
        utils/bloomfilter.h
        utils/iterator.h
        utils/slice.h
        utils/option.h
        src/builder.cpp
        src/builder.h
        utils/filemeta.h
        utils/coding.h
        utils/filename.h
        utils/file.h
        src/status.h
        src/status.cpp
        src/memtable.cpp
        src/version.h
        src/vlogbuilder.h
        src/table.h
        src/cache.cpp
        src/cache.h
        src/vlogcache.cpp
        src/vlogcache.h
        src/keycache.cpp
        src/keycache.h
)

add_executable(LSMKV correctness.cc
        kvstore.cc
        kvstore.h
        kvstore_api.h
        MurmurHash3.h
        #        persistence.cc
        test.h
        utils.h
        utils/arena.cpp
        src/skiplist.h
        src/memtable.h
        src/memtable.cpp
        src/memtable.cpp
        utils/bloomfilter.h
        utils/iterator.h
        utils/slice.h
        utils/option.h
        src/builder.cpp
        src/builder.h
        utils/filemeta.h
        utils/coding.h
        utils/filename.h
        utils/file.h
        src/status.h
        src/status.cpp
        src/memtable.cpp
        src/version.h
        src/vlogbuilder.h
        src/table.h
        src/cache.cpp
        src/cache.h
        src/vlogcache.cpp
        src/vlogcache.h
        src/keycache.cpp
        src/keycache.h
)
