//
// Created by Shen on 2022/3/30.
//

#ifndef LSM_KV_TYPES_H
#define LSM_KV_TYPES_H

#include <queue>
#include <unordered_set>
#include <list>
const std::string NOTFOUND =  "";
const std::string DELETED  = "~DELETED~";

typedef uint64_t timeStamp_t;
typedef uint64_t key_t;
typedef std::string value_t;
typedef std::pair<key_t,value_t> kv_t;

//auto KVcmp = [](const kv_t& p1, const kv_t& p2)->bool{return p1.first >p2.first;};
struct KVcmp{
    bool operator()(const kv_t& p1, const kv_t& p2){return p1.first >p2.first;}
};

//scan需要返回排序后的结果 而所有的key可能来自memtable以及不同的sstable 选择用堆排序
//不同的sstable可能有同样的key 需要根据时间戳判断哪个生效
//stl的数据结构在push相同的key时不会进行覆盖或是其他操作 更何况这里的kv时自定义结构
//解决方法1：时间戳从大到小遍历sstable push前判重 不重复才插入
//解决方法2：时间戳从小到大遍历sstable 每次push都覆盖value 但这好像不符合stl自带容器的操作  需要先用一个自己的容器装起来 最后再进行堆排序
//这里选择继承后重写push方法 使用效率更高的unordered_set(使用哈希而不是平衡树)进行key的判重
class KVheap : public std::priority_queue<kv_t,std::vector<kv_t>, KVcmp>{
private:
    std::unordered_set<key_t> keySet;
public:
    KVheap()=default;
    ~KVheap()=default;
    void push(const kv_t& kv)
    {
        if(keySet.count(kv.first)==0){
            keySet.insert(kv.first);
            std::priority_queue<kv_t,std::vector<kv_t>, KVcmp>::push(kv);
        }
    }
    void toList(std::list<kv_t> &list){
        while(!empty()){
            list.emplace_back(top());
            pop();
        }
    }
};













#endif //LSM_KV_TYPES_H
