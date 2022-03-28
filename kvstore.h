#pragma once

#include "kvstore_api.h"
#include "SkipList.h"
#include "BloomFilter.h"
#include <fstream>
#include <utility>

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
    };//sizeof(Index) != 8+4  对齐！
    struct Cache{
        SSTheader *header;
        BloomFilter *bloomFilter;
        Index *indexList;
        std::string path;
        Cache(SSTheader *h,BloomFilter *b, Index*i,std::string p):header(h),bloomFilter(b),indexList(i),path(std::move(p)){}
    };

    std::list<Cache> cacheList;
    timeStamp_t timeStamp;
    SkipList *memTable;

    void memCompaction();

    void findTimeStamp();
    //若找到key 返回true 并传递位置与长度 如果长度是-1表示到文件末尾
    static bool getValueInfo(const Cache &cache,const key_t &key,unsigned int &offset,int &length);
    static value_t getValueFromSST(const timeStamp_t &timeStamp,const unsigned int &offset,const int &length) ;
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
