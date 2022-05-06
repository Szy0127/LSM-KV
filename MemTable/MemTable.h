//
// Created by Shen on 2022/5/6.
//

#ifndef LSM_KV_MEMTABLE_H
#define LSM_KV_MEMTABLE_H

#include "utils/types.h"

class MemTable{
public:
    MemTable()= default;
    virtual ~MemTable(){};

    //5个kvstore需要实现的接口 memtable也需要实现

    virtual void put(const key_t &key,const value_t &value) = 0;
    virtual value_t get(const key_t &key)const= 0;
    virtual bool del(const key_t &key) = 0;
    virtual void scan(const key_t &key1,const key_t &key2,KVheap &heap) = 0;
    virtual void reset()=0;

    //为了判断有没有超2M memtable需要提供数量和长度去计算
    virtual uint64_t getLength()const=0;
    virtual unsigned int getSize()const=0;

    //提供存储的所有kv信息
    virtual void getList(std::list<kv_t> &list)=0;

    //为了不超2M需要自制撤回功能
    virtual void undo(const key_t &key) = 0;
};
#endif //LSM_KV_MEMTABLE_H
