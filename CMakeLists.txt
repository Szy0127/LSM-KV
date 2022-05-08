cmake_minimum_required(VERSION 3.21)
project(lsm_kv)

set(CMAKE_CXX_STANDARD 14)

include_directories(.)

add_executable(lsm_kv
        test/test.h
        test/correctness.cc
#        main.cpp
        kvstore.cc
        kvstore.h
        kvstore_api.h
        BloomFilter/BloomFilter.h
        BloomFilter/BloomFilter.cpp
        utils/MurmurHash3.h
        MemTable/MemTable.h
        MemTable/Mymap.h
        MemTable/SkipList/SkipList.cpp
        MemTable/SkipList/SkipList.h
        utils/types.h
        utils/utils.h
        )
