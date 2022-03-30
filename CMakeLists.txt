cmake_minimum_required(VERSION 3.21)
project(lsm_kv)

set(CMAKE_CXX_STANDARD 14)

include_directories(.)

add_executable(lsm_kv
#        correctness.cc
        kvstore.cc
        kvstore.h
        kvstore_api.h
        SkipList.cpp
        SkipList.h
        BloomFilter.h
        BloomFilter.cpp
        MurmurHash3.h
        main.cpp
        persistence.cc
        test.h
        #utils.h
        )
