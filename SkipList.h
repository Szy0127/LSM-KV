#pragma once

#include <utility>
#include <vector>
#include <climits>
#include <time.h>
#include <cstdint>
#include <iostream>
#include <list>

#define NOTFOUND ""

typedef uint64_t key_t;
typedef std::string value_t;
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


    key_t key_min;
    key_t key_max;
    uint64_t length; // 跳表中储存的元素个数 如果不在这里维护 需要在put中传递是否非覆盖的信息
    unsigned int value_size;//所有的字符串长度总和 如果不在这里维护 需要在put中传递增加/减少size的信息

    value_t redo_value;//先插入 如果超过阈值需要撤回  上一步没有覆盖 直接删 有覆盖 恢复

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

    void put(uint64_t key, const std::string &value);

    std::string get(uint64_t key);//返回查找路径长度
    bool del(uint64_t key);

    void scan(uint64_t key1, uint64_t key2, std::list<std::pair<uint64_t, std::string>> &list);

    void reset();
    void redo(key_t key);

    void getList(std::list<std::pair<uint64_t, std::string>> &list);

    void getRange(uint64_t &min, uint64_t &max) const;
    uint64_t getLength() const;
    unsigned int getSize() const;
};
