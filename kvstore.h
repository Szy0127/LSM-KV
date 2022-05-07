#pragma once

#include "kvstore_api.h"
#include "MemTable/SkipList/SkipList.h"
#include "BloomFilter/BloomFilter.h"
#include "utils/utils.h"
#include "utils/types.h"
#include <fstream>
#include <utility>
#include <map>
#include <queue>
#include <memory>



class KVStore : public KVStoreAPI {
	// You can add your implementation here
public:
    static const int HEADER_SIZE = 32;
    static const int BF_SIZE = 10240;
    static const int MAX_SSTABLE_SIZE = 2*1024*1024; //必须大于32+10240  单位byte
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
        unsigned int offset;// 后面4byte的pad数据无法控制
    };//为了存储的方便 把key和offset定义成一个结构体 但是这样实际上浪费了空间 因为8byte和4byte需要对齐 pad4byte
    struct CacheSST{
        std::string path;//对应sstable的路径
        SSTheader *header;
        BloomFilter *bloomFilter;
        Index *indexList;//数组
        CacheSST(std::string p,SSTheader *h,BloomFilter *b, Index*i):path(std::move(p)),header(h),bloomFilter(b),indexList(i){}
        ~CacheSST(){
            delete header;
            delete bloomFilter;
            delete[] indexList;
        }
    };
    //因为index的个数事先是不确定的  所以只能存一个数组的首地址 数量通过header.length去获取
    //虽然Cache一共只有24+path.length bytes 但是new分配到堆上的内存占了32+10240+16*length
    //理论上一个Cache占1K-300K

    std::string dataPath;//存储数据的根目录

    //搜索时从时间戳最大的开始搜索
    //向下合并时选择时间戳最小的若干个
    using CacheLevelTime = std::multimap<timeStamp_t,std::shared_ptr<CacheSST>,std::greater<>>;

    //合并时下层需要按key排序 除了第0层可能会重复  其他都不会重复 但是为了复用代码 统一使用multimap
    using CacheLevelKey = std::multimap<key_t,std::shared_ptr<CacheSST>,std::less<>>;

    std::vector<CacheLevelTime> cacheAllSST;
//    std::vector<CacheLevelKey> cacheKey;//不维护每一层按key排序  在compaction过程中动态生成需要合并的内容

    timeStamp_t fileSuffix;//每个文件加一个全局唯一id方便判断
    timeStamp_t timeStamp;//新插入元素的时间戳
    timeStamp_t timeCompaction;//合并时最大的时间戳；
    MemTable *memTable;
    int compactionLevel;//正常情况是0 表示在level0插入memTable满了之后的sstable

    static bool isIntersect(key_t &l1,key_t &r1, key_t &l2, key_t &r2);//判断两个区间是否相交


    //memtable-->sstable
    void memCompaction();

    //0-->1 compaction   4路
    // i--> i+1 (i>=1) 2路
    void compaction(int level);

    //上层与下层归并排序 由于key不相交 最多2路 最后一层需要删delete
    void mergeSort2Ways(CacheLevelKey &cacheLevelKeyUp,CacheLevelKey &cacheLevelKeyDown,bool last_level);

    //第0层到第1层需要4路归并 KeyUp一定是3个
    void mergeSort4Ways(CacheLevelKey &cacheLevelKeyUp,CacheLevelKey &cacheLevelKeyDown,bool last_level);
    static int maxLevelSize(int level);

    //搜索key时二分查找index
    static uint64_t binarySearchGet(const Index* indexList, uint64_t length, const key_t &key);
    static uint64_t binarySearchScan(const Index* indexList, uint64_t length, const key_t &key,bool begin);

    //若找到key 返回true 并传递位置与长度
    //由于length由后一个的offset计算出 最后一个无法知道长度 如果使用-1表示需要用signed 导致narrow的问题 所以增加last变量
    static bool getValueInfo(const CacheSST &cache,key_t key,unsigned int &offset,unsigned int &length,bool &last);

    //loadSST时使用
    static std::shared_ptr<CacheSST> getCacheFromSST(const std::string &path);

    //last是表示scan有没有扫描到文件末尾 最后一个的读取处理方式不同
    static void scanInSST(const std::string &path,const Index* indexList,const uint64_t &begin,const uint64_t &end,bool last,KVheap &heap);

    //初始化时使用
    void loadSST();

    //在确定好位置的情况下直接读取指定长度 如果length是-1表示最后一个（文件末尾）
    static value_t readStringFromFileByLength(std::ifstream &f, unsigned int length,bool last=false);

    //mergeSort的时候辅助使用
    static void updateIterInfo(std::shared_ptr<CacheSST> &cache,timeStamp_t &t,std::ifstream &f,uint64_t &size,Index* &indexList);
    //compaction结束 删除文件 同时删除缓存
    void removeSST(CacheLevelKey::iterator &cacheIt,int level);
public:
	KVStore(const std::string &dir);

	~KVStore();

	void put(key_t key, const value_t &s) override;

	value_t get(key_t key) override;

	bool del(key_t key) override;

	void reset() override;

	void scan(key_t key1, key_t key2, std::list<kv_t> &list) override;

};
