cmake_minimum_required(VERSION 3.22.1)
project(LSMKV.handout)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_BUILD_TYPE Debug)


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
        kvstore_api.h
        MurmurHash3.h
        utils.h
        utils/arena.cpp
        src/skiplist.h
        src/memtable.h
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
add_executable(TEST lsm_test.cpp ${SRC_FILES})

add_executable(LSMKVC correctness.cc test.h ${SRC_FILES})
add_executable(LSMKVP persistence.cc test.h ${SRC_FILES})

