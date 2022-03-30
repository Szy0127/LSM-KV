//
// Created by Shen on 2022/3/10.
//

#include "BloomFilter.h"
#include <iostream>
#include <cassert>

#define ln2 0.6931471805599453
//误报率 1/2**k 很小

BloomFilter::BloomFilter() = default;
BloomFilter::BloomFilter(int n):k(ln2*M/n+1),bitmap{0}
{
    k = k>K_MAX?K_MAX:k;
//    std::cout<<k<<std::endl;
//    bitmap = new int[M / bitPerSpace + 1]();//加括号会全部初始化为0  否则是随机的 会影响结果！！
}

BloomFilter::~BloomFilter()=default;

unsigned int BloomFilter::hash(key_t key, int index) {
    unsigned int res[4] = {0};
    MurmurHash3_x64_128(&key, sizeof(key), index, res);

    return (res[0]+res[1]+res[2]+res[3])%M;
}

void BloomFilter::insert(key_t key) {
    for (int i = 0; i < k; i++) {
        unsigned int loc = hash(key,i);
        bitmap[loc / bitPerSpace] |= 1 << (loc % bitPerSpace);
    }
    //assert(contains(key));
}

bool BloomFilter::contains(key_t key) {
    for (int i = 0; i < k; i++) {
        unsigned int loc = hash(key,i);
        if (!(bitmap[loc / bitPerSpace] & 1 << (loc % bitPerSpace))) {
            return false;
        }
    }
    return true;
}
