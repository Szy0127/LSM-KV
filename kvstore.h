#pragma once

#include "kvstore_api.h"
#include "SkipList.h"
#include "BloomFilter.h"
#include "utils.h"
#include <fstream>
#include <utility>
#include <map>

#define NOTFOUND ""
#define DELETE "~DELETED~"
typedef uint64_t timeStamp_t;
typedef uint64_t key_t;
typedef std::string value_t;

class KVStore : public KVStoreAPI {
	// You can add your implementation here
public:
    static const int HEADER_SIZE = 32;
    static const int BF_SIZE = 10240;
    static const int MAX_SSTABLE_SIZE = 2*1024*1024; //必须大于32+10240
    static std::string SUFFIX;
private:
    struct SSTheader{
        timeStamp_t  timeStamp;
        uint64_t length;
        key_t key_min;
        key_t key_max;
        SSTheader()= default;
        SSTheader(timeStamp_t t,uint64_t l,key_t kmin,key_t kmax):timeStamp(t),length(l),key_min(kmin),key_max(kmax){}
    };
    struct Index{
        key_t key;
        unsigned int offset;
    };//为了存储的方便 把key和offset定义成一个结构体 但是这样实际上浪费了空间 因为8byte和4byte需要对齐 pad4byte
    struct Cache{
        std::string path;//对应sstable的路径
        SSTheader *header;
        BloomFilter *bloomFilter;
        Index *indexList;//数组
        Cache(std::string p,SSTheader *h,BloomFilter *b, Index*i):path(std::move(p)),header(h),bloomFilter(b),indexList(i){}
    };
    //因为index的个数事先是不确定的  所以只能存一个数组的首地址 数量通过header.length去获取
    //虽然Cache一共只有24+path.length bytes 但是new分配到堆上的内存占了32+10240+16*length
    //理论上一个Cache占1K-300K


    std::string dataPath;//存储数据的根目录

    //遍历缓存 如果找到key 需要去文件读value 所以需要知道文件的完整路径
    //如果用路径作为key 需要自定义排序函数 可能插入操作会比较费时
    std::map<timeStamp_t ,Cache,std::greater<>> caches;//key是path
    timeStamp_t timeStamp;
    SkipList *memTable;

    void memCompaction();

    //若找到key 返回true 并传递位置与长度 如果长度是-1表示到文件末尾
    static bool getValueInfo(const Cache &cache,const key_t &key,unsigned int &offset,int &length);
    static value_t getValueFromSST(const std::string &path,const unsigned int &offset,const int &length);

    void getCacheFromSST(const std::string &path);
public:
	KVStore(const std::string &dir);

	~KVStore();

	void put(key_t key, const value_t &s) override;

	std::string get(key_t key) override;

	bool del(key_t key) override;

	void reset() override;

	void scan(key_t key1, key_t key2, std::list<std::pair<key_t, std::string> > &list) override;

    void read(std::string path);
};
