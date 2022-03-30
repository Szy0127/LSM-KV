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
#include <queue>
#include <algorithm>
#include <unordered_set>

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

void testSet(){
    std::unordered_set<int> a;
    std::set<int>b;
    int n = 100000;
    for(int i = 0 ;i < n ;i++){
        a.insert(i);
        b.insert(i);
    }

    auto t1 = std::chrono::high_resolution_clock::now();
    for(int i = 0 ;i < n ;i++){
        if(a.count(i)==0){
            std::cout<<"wrong";
        }
    }
    auto t2 = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1).count();
    std::cout<<duration<<std::endl;

    t1 = std::chrono::high_resolution_clock::now();
    for(int i = 0 ;i < n ;i++){
        if(b.count(i)==0){
            std::cout<<"wrong";
        }
    }
    t2 = std::chrono::high_resolution_clock::now();
    duration = std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1).count();
    std::cout<<duration<<std::endl;

}

class mySet:public std::set<int>{
public:
    mySet()=default;
    ~mySet()=default;
    void insert(int a){
        if(a!=1){
            std::set<int>::insert(a);
        }
    }
};
//int main() {
////    testKV();
////    testBF();
////    testMap();
////    testSet();
//    mySet s;
//    s.insert(2);
//    s.insert(3);
//    s.insert(1);
//    for(auto i:s){
//        std::cout<<i;
//    }
//}
//
