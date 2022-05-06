cmake_minimum_required(VERSION 3.21)
project(lsm_kv)

set(CMAKE_CXX_STANDARD 14)

include_directories(.)

add_executable(lsm_kv
#        test/correctness.cc
        test/persistence.cc
#        main.cpp
        kvstore.cc
        kvstore.h
        kvstore_api.h
        BloomFilter/BloomFilter.h
        BloomFilter/BloomFilter.cpp
        utils/MurmurHash3.h
        MemTable/MemTable.h
        MemTable/SkipList/SkipList.cpp
        MemTable/SkipList/SkipList.h
        utils/types.h
        test/test.h
        utils/utils.h
        )
