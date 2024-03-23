cmake_minimum_required(VERSION 3.24)
project(LSMKV.handout)

set(CMAKE_CXX_STANDARD 17)

add_executable(LSMKV.handout correctness.cc
        kvstore.cc
        kvstore.h
        kvstore_api.h
        MurmurHash3.h
        persistence.cc
        test.h
        utils.h
        utils/arena.cpp
        skiplist.h)
