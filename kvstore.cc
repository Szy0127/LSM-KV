#include "kvstore.h"
#include <string>
#include <cassert>


std::string KVStore::SUFFIX = ".sst";

KVStore::KVStore(const std::string &dir) : KVStoreAPI(dir), dataPath(dir),compactionLevel(0) {
    if (dataPath.back() != '/') {
        dataPath.push_back('/');
    }
    memTable = new SkipList;
    //测试的时候每次是从头开始的 需要把本地的文件删干净 或者不进行读入的操作 或者修改测试的代码 加reset
    loadSST();
}

void KVStore::loadSST()
{
    //把所有sstable的索引部分都读入缓存 并且找到最后一个时间戳
    std::vector<std::string> levelDirs;
    std::vector<std::string> sstables;
    std::string levelPath;
    utils::scanDir(dataPath, levelDirs);
    int level_count = 0;
    for (auto &level: levelDirs) {
        if(cacheTime.size() < level_count+1){
            cacheTime.emplace_back(CacheLevelTime());
            cacheKey.emplace_back(CacheLevelKey());
        }
        levelPath = dataPath + level + "/";
        sstables.clear();
        utils::scanDir(levelPath, sstables);
        for (auto &sstable: sstables) {
            timeStamp_t t = std::stoull(sstable.substr(0, sstable.find(SUFFIX)));
            if (t > timeStamp) {
                timeStamp = t;
            }
            std::shared_ptr<CacheSST> cache = getCacheFromSST(levelPath + sstable);
            cacheTime[level_count].insert(std::make_pair(t,cache));
            cacheKey[level_count].insert(std::make_pair(cache->header->key_min,cache));
        }
        level_count++;
    }
    cacheTime.emplace_back(CacheLevelTime());
    cacheKey.emplace_back(CacheLevelKey());
}

KVStore::~KVStore() {
    if (memTable->getLength() > 0) {
        memCompaction();
    }
    delete memTable;
}

/**
 * Insert/Update the key-value pair.
 * No return values for simplicity.
 */
void KVStore::put(key_t key, const value_t &s) {
    //先插入 这样是否超出阈值的结果是显然的  如果超出就撤回这次操作
    //否则计算的结果会基于是否覆盖 需要进入memtable的插入过程 很麻烦
    memTable->put(key, s);
    if (HEADER_SIZE + BF_SIZE + sizeof(Index) * (memTable->getLength()) + memTable->getSize() >= MAX_SSTABLE_SIZE) {
        memTable->undo(key);
        memCompaction();
        memTable->reset();
        memTable->put(key, s);
    }
}


/**
 * Returns the (string) value of the given key.
 * An empty string indicates not found.
 */

std::string KVStore::get(key_t key) {
    std::string memRes = memTable->get(key);
    //memTable里的数据一定是最新的 如果找到/或者删了 肯定就是对的
    if (memRes == DELETED) {
        return NOTFOUND;
    }
    if (memRes != NOTFOUND) {
        return memRes;
    }
    //如果memTable里没有这个数据 去缓存找
    //缓存有两种类型 第一种是之前存的sstable 会在这次lsm启动之前就读入缓存
    //第二种是这次启动后写入sstable的  两种情况的sstable的信息全都在缓存里
    unsigned int offset;
    int length;
    bool find;
//    std::cout<<key<<std::endl;
    for (auto const &cacheLine: cacheTime) {
        for(auto const &cache:cacheLine){
            //在找到的同时拿到位置与大小信息
//        std::cout<<cache.first<<" "<<cache.second.header->timeStamp<<std::endl;
            find = getValueInfo(*cache.second, key, offset, length);
            if (find) {
                //根据文件名 位置 大小 直接读出信息
                std::string res = getValueFromSST(cache.second->path, offset, length);
                if (res == DELETED) {
                    return NOTFOUND;
                }
                return res;
            }
    }

    }
    return NOTFOUND;

}

/**
 * Delete the given key-value pair if it exists.
 * Returns false iff the key is not found.
 */
bool KVStore::del(key_t key) {
//    return memTable->del(key);
    if (get(key) == NOTFOUND) {
        return false;//跳表和sstable中都没找到
    }
    if (memTable->get(key) != NOTFOUND) {
        memTable->del(key);//在跳表里 可以直接删
        return true;
    }
    memTable->put(key, DELETED);
    return true;
}

/**
 * This resets the kvstore. All key-value pairs should be removed,
 * including memtable and all sstables files.
 */
void KVStore::reset() {
    timeStamp = 0;
    compactionLevel = 0;
    memTable->reset();
    cacheTime.clear();
    cacheKey.clear();
    std::vector<std::string> levelDir, files;
    std::string levelPath;
    utils::scanDir(dataPath, levelDir);
    for (auto &level: levelDir) {
        levelPath = dataPath + level + '/';
        utils::scanDir(levelPath, files);
        for (auto &file: files) {
//            std::cout<<file<<" ";
            utils::rmfile((levelPath + file).c_str());
        }
        utils::rmdir(levelPath.c_str());
    }
    cacheTime.emplace_back(CacheLevelTime());
    cacheKey.emplace_back(CacheLevelKey());
}

/**
 * Return a list including all the key-value pair between key1 and key2.
 * keys in the list should be in an ascending order.
 * An empty string indicates not found.
 */
void KVStore::scan(key_t key1, key_t key2, std::list<kv_t> &list) {
//    std::cout << "scan:" << key1 << " " << key2 << std::endl;
    KVheap scanResult;
    memTable->scan(key1, key2, scanResult);

    for (auto const &cacheLine: cacheTime) {{
        for(auto const&cache:cacheLine){
            if (key1 > cache.second->header->key_max || key2 < cache.second->header->key_min) {
                continue;
            }
            //第一个大于等于key1的key的位置
            uint64_t begin = key1 <= cache.second->header->key_min ? 0 : binarySearchScan(cache.second->indexList,
                                                                                         cache.second->header->length,
                                                                                         key1, true);
            uint64_t end = key2 >= cache.second->header->key_max ? cache.second->header->length - 1 : binarySearchScan(
                    cache.second->indexList, cache.second->header->length, key2, false);
            scanInSST(cache.second->path, cache.second->indexList, begin, end, end == cache.second->header->length - 1,
                      scanResult);
//        std::cout << cache.second.header->key_min << " " << cache.second.header->key_max << " " << begin << " " << end<< std::endl;
        }
        }
    }

    scanResult.toList(list);
}

void KVStore::memCompaction() {
    /*
     * 这里为了复用代码 有两种情况
     * 1 正常put 满了以后会compaction到第0层 向第1层检查
     * 2 在compaction的时候借用put函数 满了之后写入第level层 不用向第1层检查
     * 为了不影响接口 选择使用类中成员变量的方式进行条件的传递
     * compactionLevel:表示当前即将写入的sstable存在level层
     * timeCompation：若为0 当前写入的文件用最新的 若不为0 表示是compation 用之前的
     */
    std::list<std::pair<uint64_t, std::string>> list;
    memTable->getList(list);
// 头部 时间戳 数量 最大值 最小值 各8bytes
    timeStamp++;
    uint64_t length;
    key_t key_min, key_max;

    length = memTable->getLength();
    //跳表本身是有序排列的  所以不需要动态维护key的范围 stl的list是双向链表 所以back应该不需要遍历
    key_min = list.front().first;
    key_max = list.back().first;


    std::string path = dataPath + "level-"+std::to_string(compactionLevel)+"/";
    if (!utils::dirExists(path)) {
        utils::mkdir(path.c_str());
    }
    path.append(std::to_string(timeCompaction?timeCompaction:timeStamp).append(SUFFIX));
    std::ofstream f(path, std::ios::binary);

//    std::cout << "[info]writeSST:" << timeStamp << " " << length << " " << key_min << " " << key_max << std::endl;
    SSTheader *header = new SSTheader(timeStamp, length, key_min, key_max);
    f.write((char *) header, HEADER_SIZE);


    // 布隆过滤器 10240bytes
    BloomFilter *bloomFilter = new BloomFilter(length);
    for (auto const &kv: list) {
//        std::cout << kv.first << " " << kv.second << std::endl;
        bloomFilter->insert(kv.first);
    }
    f.write((char *) bloomFilter, sizeof(BloomFilter));


//     key_offset 共(8+4)*length bytes offset是value的绝对定位
    Index *indexList = new Index[length];
    unsigned int value_offset = HEADER_SIZE + BF_SIZE + sizeof(Index) * length;
    int i = 0;
    for (auto const &kv: list) {
        indexList[i].key = kv.first;
        indexList[i].offset = value_offset;
        i++;
        value_offset += kv.second.length();
    }
    f.write((char *) indexList, sizeof(Index) * length);

//    cacheList.emplace_back(Cache(header, bloomFilter, indexList, path));
//    caches[path] = Cache(header, bloomFilter, indexList);
    std::shared_ptr<CacheSST> cache = std::shared_ptr<CacheSST>(new CacheSST(path, header, bloomFilter, indexList));

    cacheTime[compactionLevel].insert(std::make_pair(timeStamp, cache));
    cacheKey[compactionLevel].insert(std::make_pair(key_min, cache));
    //所有value连续存
    for (auto const &kv: list) {
//        f.write((char*)&kv.second,kv.second.length());
// 这里用f.write会乱 用流输出就正常
        f << kv.second;
    }

    f.close();
//    read(path);

    //compactionLevel==0 正常插入
    //!=0 合并时借助同一个函数生成sstable
    if(compactionLevel == 0 && cacheTime[0].size() > maxLevelSize(0)){
        zeroCompaction();
    }

}
void KVStore::zeroCompaction() {
    compaction(0);
}

void KVStore::compaction(int level) {//level满了 向level+1合并
    int max = maxLevelSize(level);
    if(cacheTime[level].size() <= max){
        return;//不需要compaction  结束递归
    }
    int count = 1;
    CacheLevelKey cacheLevelKey;
    for(auto const &cache:cacheTime[level]){//取时间戳最小的若干个  也就是最后的
        if(count > max){
            cacheLevelKey.insert(std::make_pair(cache.second->header->key_min,cache.second));
        }
        count++;
    }

    if(cacheTime.size() <= level+1){
        cacheTime.emplace_back(CacheLevelTime());
        cacheKey.emplace_back(CacheLevelKey());
        std::string dirPath = dataPath+"level-"+std::to_string(level+1);
        if(!utils::dirExists(dirPath)){
            utils::mkdir(dirPath.c_str());
        }
    }

    auto itup = cacheLevelKey.begin();
    auto itdown = cacheKey[level+1].begin();

    compactionLevel = level+1;
    for(;itup!=cacheLevelKey.end();itup++){
        std::shared_ptr<CacheSST> cache = itup->second;
        timeCompaction = cache->header->timeStamp;
        std::ifstream f(cache->path);
        int size = cache->header->length;
        Index *indexList = cache->indexList;
        key_t key;
        value_t value;
        unsigned int offset;
        int length;
        char *c = nullptr;
        for(int i = 1; i < size ; i++){
            key = indexList[i-1].key;
            offset = indexList[i-1].offset;
            length = indexList[i].offset - offset;
            f.seekg(offset);
            c = new char[length+1];
            c[length] = '\0';
            value = std::string(c);
            delete c;
            put(key,value);
        }
        f>>value;
        put(key,value);
        f.close();
    }
    if(memTable->getLength()>0){
        memCompaction();
    }
    compactionLevel = 0;
    timeCompaction = 0;
    if(level==1){
//        std::cout<<cacheLevelKey.begin()->second->header->timeStamp<<std::endl;
    }
    //这些多出来的要从原来的缓存和文件中删除
    for(auto const &cache:cacheLevelKey){
        cacheTime[level].erase(cache.second->header->timeStamp);
        cacheKey[level].erase(cache.first);
        utils::rmfile(cache.second->path.c_str());
    }

    compaction(level+1);
}

void testBF(BloomFilter &bloomFilter, uint64_t begin, uint64_t end) {
    for (uint64_t i = begin; i < end; i++) {
        if (!bloomFilter.contains(i)) {
            std::cout << "wrong" << std::endl;
        }
    }
    int wrong = 0;
    for (uint64_t i = end * 100; i < 200 * end; i++) {
        if (bloomFilter.contains(i)) {
            wrong++;
        }
    }
    std::cout << (double) wrong / end * 100 << std::endl;
}

void KVStore::read(std::string path) {

    std::ifstream f(path, std::ios::binary);

    SSTheader header;

    f.read((char *) &header, HEADER_SIZE);

//    std::cout << "read:" << header.length << " " << header.key_min << " " << header.key_max << std::endl;
    BloomFilter bloomFilter(header.length);
    f.read((char *) &bloomFilter, sizeof(BloomFilter));
//    testBF(bloomFilter,header.key_min,header.key_max);

    Index indexList[header.length];
    f.read((char *) indexList, sizeof(Index) * header.length);

    unsigned int value_length_max = 2048;
    unsigned int value_length;
    char *buffer = new char[value_length_max];
    for (int i = 1; i < header.length; i++) {
        value_length = indexList[i].offset - indexList[i - 1].offset;
        if (value_length > value_length_max) {
            value_length_max = value_length * 2;
            delete[] buffer;
            buffer = new char[value_length_max];
        }
        f.read(buffer, value_length);
        buffer[value_length] = '\0';
        std::string value(buffer);
//        std::cout<<indexList[i-1].key<<" "<<value<<std::endl;
    }
    std::string value;
    f >> value;
//    std::cout<<indexList[header.length-1].key<<" "<<value;

    f.close();

}

std::shared_ptr<KVStore::CacheSST> KVStore::getCacheFromSST(const std::string &path) {
    std::ifstream f(path, std::ios::binary);

    SSTheader *header = new SSTheader();
    f.read((char *) header, HEADER_SIZE);

//    std::cout << "read:" << header.length << " " << header.key_min << " " << header.key_max << std::endl;
    BloomFilter *bloomFilter = new BloomFilter();
    f.read((char *) bloomFilter, sizeof(BloomFilter));

    Index *indexList = new Index[header->length];
    f.read((char *) indexList, sizeof(Index) * header->length);

//    caches[path] = Cache(header,bloomFilter,indexList);//这样会报错 需要默认构造函数 可能是复制过去？
    f.close();
    return std::shared_ptr<CacheSST>(new CacheSST(path, header, bloomFilter, indexList));
}

uint64_t KVStore::binarySearchGet(const Index *const indexList, uint64_t length, const key_t &key) {
    uint64_t l = 0;
    uint64_t r = length - 1;
    uint64_t m;
    while (l <= r) {
        m = (l + r) / 2;
        if (indexList[m].key == key) {
            return m;
        }
        if (indexList[m].key < key) {
            l = m + 1;
        } else {
            r = m - 1;
        }
    }
    return length;//没找到
}

uint64_t KVStore::binarySearchScan(const Index *const indexList, uint64_t length, const key_t &key, bool begin) {
    uint64_t l = 0;
    uint64_t r = length - 1;
    uint64_t m;
    while (r - l > 1) {
        m = (l + r) / 2;
        if (indexList[m].key == key) {
            return m;
        }
        if (indexList[m].key < key) {
            l = m;
        } else {
            r = m;
        }
    }
    if (begin) {
        return r;
    }
    return l;
}

bool KVStore::getValueInfo(const CacheSST &cache, const key_t &key, unsigned int &offset, int &length) {
//    std::cout<<"[info]is finding "<<key<<std::endl;
    // 检查范围
    if (key < cache.header->key_min || key > cache.header->key_max) {
        return false;
    }
    //检查BF  BF没有一定没有 BF有实际没有只会多花时间
    if (!cache.bloomFilter->contains(key)) {
        return false;
    }

    //二分查找 可能找不到
    uint64_t loc = binarySearchGet(cache.indexList, cache.header->length, key);
    if (loc >= cache.header->length) {
        return false;
    }
    offset = cache.indexList[loc].offset;
    if (loc < cache.header->length - 1) {
        length = cache.indexList[loc + 1].offset - offset;
    } else {
        length = -1;
    }
    return true;
}

//在指定位置读指定长度个字符 返回字符串
//想到的方法只有seekg+read    char[]转string
value_t KVStore::getValueFromSST(const std::string &path, const unsigned int &offset, const int &length) {
//    std::cout<<"[info]is finding in "<<timeStamp<<" offset/length:"<<offset<<" "<<length<<std::endl;
    std::ifstream f(path, std::ios::binary);
    f.seekg(offset);
    if (length < 0) {//最后一个不知道长度 但是可以直接全部输入
        std::string v;
        f >> v;
        return v;
    }

    char *c = new char[length + 1];
    f.read(c, length);
    c[length] = '\0';
    std::string v(c);

    delete[] c;
    f.close();//不close会有问题
    return v;
}

void
KVStore::scanInSST(const std::string &path, const Index *const indexList, const uint64_t &begin, const uint64_t &end,
                   bool last, KVheap &heap) {
    std::ifstream f(path, std::ios::binary);
    unsigned int offset = indexList[begin].offset;
    f.seekg(offset);
    unsigned int length;
    char *c;
    for (uint64_t i = begin; i < end; i++) {
        length = indexList[i+1].offset - offset;
        c = new char[length + 1];
        f.read(c, length);
        c[length] = '\0';
        std::string v(c);
        if (v != DELETED) {
            heap.push(std::make_pair(indexList[i].key, v));
        }
        delete[] c;
        offset += length;
    }

    if (last) {
        std::string v;
        f >> v;
        heap.push(std::make_pair(indexList[end].key, v));
    } else {
        length = indexList[end + 1].offset - offset;
        c = new char[length + 1];
        f.read(c, length);
        c[length] = '\0';
        heap.push(std::make_pair(indexList[end].key, std::string(c)));
        delete[] c;
    }
    f.close();
}

int KVStore::maxLevelSize(int level) {
    return 1<<(level+1);
}