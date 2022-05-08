//
// Created by Shen on 2022/3/21.
//

#include <iostream>
#include "kvstore.h"

#include <map>
#include <chrono>
#include <random>
#include <algorithm>
#include <fstream>


//key范围0-1000000
std::default_random_engine randomEngine(time(nullptr));
std::uniform_int_distribution<unsigned> uniformIntDistribution(0,1000000);
std::string path = "../data/";

unsigned int inline getRandKey(){
    return uniformIntDistribution(randomEngine);
}
void test_base(int length,std::ofstream &f){
    f<<"length:"<<length<<std::endl;
    std::vector<key_t> test_data;
    for(int i = 0 ; i < length ;i++){
        test_data.emplace_back(getRandKey());
    }
    //必须先清空上次的数据
    KVStore kvStore(path);
    kvStore.reset();

    //先插入 越到后面越慢  但是统计平均时间
    auto t1 = std::chrono::high_resolution_clock::now();
    for(int i = 0 ;i < length ;i++){
        kvStore.put(test_data[i], std::string(1000,'s'));
    }
    auto t2 = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1).count();
    f<<"put latency:"<<(double)duration/length<<std::endl;

    //查找 先找所有的key 然后找不存在的key
    t1 = std::chrono::high_resolution_clock::now();
    for(int i = 0 ;i < length ;i++){
        kvStore.get(test_data[i]);
    }
    //模拟一些key不存在的情况
    for(int i = 0 ;i < length/5 ;i++){
        kvStore.get(getRandKey());
    }
    t2 = std::chrono::high_resolution_clock::now();
    duration = std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1).count();
    f<<"get latency:"<<(double)duration/length/1.2<<std::endl;

    // 删除 先删一些不存在的  然后全删了
    for(int i = 0 ;i < length/5 ;i++){
        kvStore.del(getRandKey());
    }
    t1 = std::chrono::high_resolution_clock::now();
    for(int i = 0 ;i < length ;i++){
        kvStore.del(test_data[i]);
    }

    t2 = std::chrono::high_resolution_clock::now();
    duration = std::chrono::duration_cast<std::chrono::microseconds>(t2 - t1).count();
    f<<"del latency:"<<(double)duration/length/1.2<<std::endl;

}
int main() {
//    std::ofstream f("../test_res/test_base_small.txt");
    std::ofstream f("../test_res/test_base_big.txt");
//    std::vector<int> test_length_small{1000,1500,2000,2500,3000,3500,4000,4500,5000,8000,10000};
    std::vector<int> test_length_big{5000,10000,20000,50000,80000,100000};
    for(auto i:test_length_big){
        test_base(i,f);
    }
    f.close();
}

