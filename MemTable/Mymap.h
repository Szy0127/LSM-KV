//
// Created by Shen on 2022/5/8.
//
#include <map>
#include "MemTable.h"
#ifndef LSM_KV_MYMAP_H
#define LSM_KV_MYMAP_H

class Mymap{
private:
    unsigned int value_size;//所有的字符串长度总和 如果不在这里维护 需要在put中传递增加/减少size的信息
    value_t overwritten;//先插入 如果超过阈值需要撤回  上一步没有覆盖 直接删 有覆盖 恢复
    std::map<key_t,value_t> _map;
public:
    Mymap():value_size(0){}
    ~Mymap(){}
    void put(const key_t &key,const value_t &value){
        value_size += value.size();
        if(_map.count(key)){
            value_size -= _map[key].size();
            overwritten = _map[key];
            _map[key] = value;
        }else{
            overwritten.clear();
            _map.insert(std::make_pair(key, value));
        }

    }
    value_t get(const key_t &key){
        if(_map.count(key)){
            return _map[key];
        }
        return NOTFOUND;
    }
    virtual bool del(const key_t &key){
        if(_map.count(key)){
            value_size -= _map[key].size();
            _map.erase(key);
            return true;
        }
        return false;
    }
    virtual void scan(const key_t &key1,const key_t &key2,KVheap &heap){
        for(auto kv:_map){
            if(kv.first >= key1 && kv.first <= key2){
                heap.push(kv);
            }
        }
    }
    virtual void reset(){
        _map.clear();
        overwritten.clear();
        value_size = 0;
    }

    //为了判断有没有超2M memtable需要提供数量和长度去计算
    virtual uint64_t getLength(){
        return _map.size();
    }
    virtual unsigned int getSize(){
        return value_size;
    }

    //提供存储的所有kv信息
    virtual void getList(std::list<kv_t> &list){
        for(auto kv:_map){
            list.emplace_back(kv);
        }
    }

    //为了不超2M需要自制撤回功能
    virtual void undo(const key_t &key){
        if (overwritten.empty()) {//上一次操作是插入 直接删
            del(key);
        } else {//上一次操作是覆盖 恢复原来的值
            put(key, overwritten);
        }
    }
};
#endif //LSM_KV_MYMAP_H
