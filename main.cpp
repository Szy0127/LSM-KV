//
// Created by Shen on 2022/3/21.
//

#include <iostream>
#include <fstream>
#include <ctime>
#include <cassert>
#include "MurmurHash3.h"
#include "BloomFilter.h"
#include "kvstore.h"
#include "utils.h"
#include <unordered_map>
#include <map>
#include <chrono>

std::string path = "../data/";

void testHash(long long key) {
    unsigned int hash[4] = {0};
    MurmurHash3_x64_128(&key, sizeof(key), (unsigned) time(nullptr), hash);
    std::cout << hash[0] << " " << hash[1] << " " << hash[2] << " " << hash[3];
}

void testInput() {
    std::ofstream f(path + "test");
    std::string a = "abc";
    std::string b = "def";
    f << a << b;
//f<<a<<'\0'<<b<<'\0';
    f.close();
}


void testOuput() {
    std::ifstream f(path + "test");
    char *a = new char[4];
    f.read(a, 3);
    a[3] = '\0';
    std::cout << a;
    f.close();
}

void testKV(){
    KVStore store("data");
    for(int i = 1 ; i <= 1000 ;i++){
        store.put(i,std::to_string(i)+std::string(i,'s'));
    }
    std::cout<<store.get(791);
    std::cout<<store.get(791);
}
void testBF()
{
    int n = 10000;
    BloomFilter bloomFilter(n);
    for(int i = 0 ; i < n ;i++){
        bloomFilter.insert(i);
    }

    int wrong = 0;
    for(int i = n ; i < 2*n ;i++){
        if(bloomFilter.contains(i)){
            wrong++;
        }
    }
    std::cout<<(double)wrong/n<<std::endl;
}

std::string SUFFIX = ".sst";
struct Key_compare
{
    bool operator () (const std::string& p1, const std::string& p2) const
    {
        timeStamp_t t1 = std::stoull(p1.substr(0, p1.find(SUFFIX)));
        timeStamp_t t2 = std::stoull(p2.substr(0, p1.find(SUFFIX)));
        return t1<t2;
    }
};

//int main() {
////    testKV();
////    testBF();
////    testMap();
//
//}

