#pragma once

#include <utility>
#include <vector>
#include <climits>
#include <time.h>
#include <cstdint>
#include <iostream>
#include <list>

// 这里的接口和kvstore_api都是一样的  但是不能继承  因为kvstore_api的构造函数是必须带文件夹路径的
//但是skiplist只是实现了内存中的操作 并不需要路径 所以还是单独写
class SkipList
{
public:
    static int MAX_LEVEL;//L(N) = log(1/p)(n)
    static double p;
private:
    enum SKNodeType
    {
        HEAD = 1,
        NORMAL,
        NIL
    };

    struct SKNode
    {
        uint64_t key;
        std::string val;
        SKNodeType type;
        std::vector<SKNode *> forwards;
        SKNode(int v,uint64_t _key, std::string _val, SKNodeType _type)
                : key(_key), val(std::move(_val)), type(_type)
        {
            for (int i = 0; i < v; ++i)
            {
                forwards.push_back(nullptr);
            }
        }
    };


    SKNode *head;
    SKNode *tail;
    std::vector<SKNode *> update;
    void update_create();
    void update_clear();
    double my_rand();
    int randomLevel();
    int level;
public:
    SkipList();
    ~SkipList();
    void put(uint64_t key, const std::string &value);
    std::string get(uint64_t key);//返回查找路径长度
    bool del(uint64_t key);
    void scan(uint64_t key1, uint64_t key2,std::list<std::pair<uint64_t,std::string>> &list);
    void reset();
    void getList(std::list<std::pair<uint64_t,std::string>> &list);
};
