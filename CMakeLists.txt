cmake_minimum_required(VERSION 3.22.1)
project(LSMKV.handout)

set(CMAKE_CXX_STANDARD 23)
set(CMAKE_BUILD_TYPE Debug)


OPTION(ENABLE_TEST "Enable test" ON)
OPTION(ENABLE_BENCHMARK "Enable benchmark" OFF)
OPTION(ENABLE_DEBUG "Enable debug" ON)
OPTION(ENABLE_ASAN "Enable AddressSanitizer" OFF)
OPTION(ENABLE_UBSAN "Enable UndefinedBehaviorSanitizer" ON)
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

SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -march=native -w -ftree-vectorize")

if (ENABLE_DEBUG)
    message(STATUS "Debug mode enabled")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g")
    SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O0 -march=native -w -ftree-vectorize")
else ()
    message(STATUS "Release mode enabled")
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O2")
endif ()

if (ENABLE_BECHMARK)
    add_subdirectory("third_party/benchmark")
    add_executable(DBBench bench/dbbench.cpp test/testutil.cc test/include/testutil.h ${SRC_FILES})
    target_link_libraries(DBBench bench benchmark_main)
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

add_subdirectory(src)






