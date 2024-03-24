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
        skiplist.h
        memtable.h
        memtable.cpp
        memtable.cpp
        utils/bloomfilter.h
        utils/iterator.h
        utils/slice.h
        utils/option.h
        builder.cpp
        builder.h
        utils/filemeta.h
)
