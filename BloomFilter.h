//
// Created by Shen on 2022/3/10.
//

#ifndef HANDIN_BLOOMFILTER_H
#define HANDIN_BLOOMFILTER_H

#include <set>
#include <vector>
#include <functional>
#include "MurmurHash3.h"
class BloomFilter {
private:
    static const int M = 81920;//哈希函数范围 即总bitmap的bit数
    static const int K_MAX = 2000;
    int k;//哈希函数个数 k=ln2(m/n)
    static const int bitPerSpace = 32;
    std::hash<std::string> hash_string;
    int *bitmap; //如果用uint64_t或者long long  bitPerSpace都是32 不知道为什么 会错误 误报率极高
    unsigned int hash(uint64_t key,int index);
public:
    BloomFilter(int n);
    ~BloomFilter();
    void insert(uint64_t key);
    bool contains(uint64_t key);
};

#endif //HANDIN_BLOOMFILTER_H
