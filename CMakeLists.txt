cmake_minimum_required(VERSION 3.24)
project(LSMKV.handout)

set(CMAKE_CXX_STANDARD 14)

add_executable(LSMKV.handout correctness.cc
        kvstore.cc
        kvstore.h
        kvstore_api.h
        MurmurHash3.h
        persistence.cc
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
        utils/writablefile.h
        src/status.h
        src/status.cpp
        src/memtable.cpp
        src/version.h
        src/vlogbuilder.h
)
