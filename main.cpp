//
// Created by Shen on 2022/3/21.
//

#include <iostream>
#include <fstream>
#include <ctime>
#include <cassert>
#include "MurmurHash3.h"
#include "kvstore.h"

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
        store.put(i,std::string(i,'s'));
    }
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

void testRW(){
    int *key = new int;
    *key = 1;
    char *c = new char[10];
    c = "abd\0cccc";
    std::string s(c);
    std::cout<<s<<" "<<s.length()<<std::endl;
    c = "e";
    std::cout<<c<<" "<<s;
    std::pair<int,std::string> b(std::make_pair(*key,s));
    std::cout<<b.first;
    *key=2;
    std::cout<<b.first;
}
int main() {
    testKV();
//    testRW();
}