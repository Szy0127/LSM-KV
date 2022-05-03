cmake_minimum_required(VERSION 3.21)
project(lsm_kv)

set(CMAKE_CXX_STANDARD 14)

include_directories(.)

add_executable(lsm_kv
#        test/correctness.cc
        test/persistence.cc
        kvstore.cc
        kvstore.h
        kvstore_api.h
        SkipList/SkipList.cpp
        SkipList/SkipList.h
        BloomFilter/BloomFilter.h
        BloomFilter/BloomFilter.cpp
        utils/MurmurHash3.h
        #main.cpp
        utils/types.h
        test/test.h
        utils/utils.h
        )
