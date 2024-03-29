#pragma once

#include <utility>
#include <vector>
#include <climits>
#include <time.h>
#include <cstdint>
#include <iostream>
#include <list>
#include "types.h"


// 这里的接口和kvstore_api都是一样的  但是不能继承  因为kvstore_api的构造函数是必须带文件夹路径的
//但是skiplist只是实现了内存中的操作 并不需要路径 所以还是单独写
class SkipList {
public:
    static int MAX_LEVEL;//L(N) = log(1/p)(n)
    static double p;
private:
    enum SKNodeType {
        HEAD = 1,
        NORMAL,
        NIL
    };

    struct SKNode {
        key_t key;
        value_t val;
        SKNodeType type;
        std::vector<SKNode *> forwards;

        SKNode(int v, key_t _key, value_t _val, SKNodeType _type)
                : key(_key), val(std::move(_val)), type(_type) {
            for (int i = 0; i < v; ++i) {
                forwards.push_back(nullptr);
            }
        }
    };


    uint64_t length; // 跳表中储存的元素个数 如果不在这里维护 需要在put中传递是否非覆盖的信息
    unsigned int value_size;//所有的字符串长度总和 如果不在这里维护 需要在put中传递增加/减少size的信息

    value_t overwritten;//先插入 如果超过阈值需要撤回  上一步没有覆盖 直接删 有覆盖 恢复
    bool rewrite;//判断是否是覆盖


    SKNode *head;
    SKNode *tail;
    std::vector<SKNode *> update;

    void update_create();

    void update_clear();

    static int randomLevel();

    int level;
public:
    SkipList();

    void init();

    ~SkipList();

    void finish();

    void put(key_t key, const value_t &value);

    value_t get(key_t key);


    //删除操作需更新key的范围
    bool del(key_t key);

    void scan(key_t key1, key_t key2, KVheap &heap);

    void reset();


    //如果上个操作是覆盖 则需要恢复value
    //如果上个操作是插入 则删除该键
    void undo(const key_t &key);

    void getList(std::list<kv_t> &list);

    uint64_t getLength() const;
    unsigned int getSize() const;
};
