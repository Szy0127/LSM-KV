//
// Created by Shen on 2022/3/10.
//

#ifndef HANDIN_BLOOMFILTER_H
#define HANDIN_BLOOMFILTER_H

#include <set>
#include <vector>
#include <functional>
#include <fstream>
#include "MurmurHash3.h"
#include "types.h"
class BloomFilter {
public:
    static const int M = (10240-4)*8;//哈希函数范围 即总bitmap的bit数
    //为了让BloomFilter这个类可以作为一个整体去存储 必须带上4bytes的哈希函数个数k 这样就少了32bit
private:
    static const int K_MAX = 2000;
    int k{};//哈希函数个数 k=ln2(m/n)
    static const int bitPerSpace = 32;
    int bitmap[M / bitPerSpace]{}; //如果用uint64_t或者long long  bitPerSpace都是32 不知道为什么 会错误 误报率极高
    static unsigned int hash(key_t key,int index);
public:
    BloomFilter(int n);
    BloomFilter();
    ~BloomFilter();
    void insert(key_t key);
    bool contains(key_t key);
};

#endif //HANDIN_BLOOMFILTER_H
