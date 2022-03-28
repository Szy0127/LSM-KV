#include "kvstore.h"
#include <string>


KVStore::KVStore(const std::string &dir) : KVStoreAPI(dir) {
//    BloomFilter::M = KVStore::BF_SIZE * 8;
    memTable = new SkipList;
    findTimeStamp();
}

KVStore::~KVStore() {
    for (auto & cache: cacheList) {
        delete cache.header;
        delete cache.indexList;
        delete cache.bloomFilter;
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
    if (HEADER_SIZE + BF_SIZE + sizeof(Index)*(memTable->getLength()) + memTable->getSize() >= MAX_SSTABLE_SIZE) {
        memTable->redo(key);
        memCompaction();
        memTable->reset();
        memTable->put(key,s);
    }
}

/**
 * Returns the (string) value of the given key.
 * An empty string indicates not found.
 */
std::string KVStore::get(key_t key) {
    std::string memRes = memTable->get(key);
    //memTable里的数据一定是最新的 如果找到/或者删了 肯定就是对的
    if (memRes == DELETE) {
        return NOTFOUND;
    }
    if (memRes != NOTFOUND) {
        return memRes;
    }
    //如果memTable里没有这个数据 先去缓存找
    unsigned int offset;
    int length;
    bool find;
    for (auto const &cache: cacheList) {
        //在找到的同时拿到位置与大小信息
        find = getValueInfo(cache, key, offset, length);
        if (find) {
            //根据文件名 位置 大小 直接读出信息
            std::string res = getValueFromSST(cache.header->timeStamp, offset, length);
            if (res == DELETE) {
                return NOTFOUND;
            }
            return res;
        }
    }

    //缓存找完去文件里找


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
    if(memTable->get(key)!=NOTFOUND){
        memTable->del(key);//在跳表里 可以直接删
        return true;
    }
    memTable->put(key, DELETE);
    return true;
}

/**
 * This resets the kvstore. All key-value pairs should be removed,
 * including memtable and all sstables files.
 */
void KVStore::reset() {
    timeStamp = 0;
    memTable->reset();
}

/**
 * Return a list including all the key-value pair between key1 and key2.
 * keys in the list should be in an ascending order.
 * An empty string indicates not found.
 */
void KVStore::scan(key_t key1, key_t key2, std::list<std::pair<key_t, value_t> > &list) {
    memTable->scan(key1, key2, list);
}

void KVStore::memCompaction() {
// 头部 时间戳 数量 最大值 最小值 各8bytes
    timeStamp++;
    uint64_t length;
    key_t key_min, key_max;
    length = memTable->getLength();
    memTable->getRange(key_min, key_max);
    std::string path = "../data/" + std::to_string(timeStamp);
    std::ofstream f(path, std::ios::binary);

//    std::cout << "[info]writeSST:" << timeStamp << " " << length << " " << key_min << " " << key_max << std::endl;
    SSTheader *header = new SSTheader(timeStamp, length, key_min, key_max);
    f.write((char *) header, HEADER_SIZE);

    std::list<std::pair<uint64_t, std::string>> list;
    memTable->getList(list);

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

    cacheList.emplace_back(Cache(header, bloomFilter, indexList, path));

    //所有value连续存
    for (auto const &kv: list) {
//        f.write((char*)&kv.second,kv.second.length());
// 这里用f.write会乱 用流输出就正常
        f << kv.second;
    }

    f.close();
//    read(path);

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

void KVStore::findTimeStamp() {
    timeStamp = 0;//遍历sstable 找到最后一个
}

bool KVStore::getValueInfo(const Cache &cache, const key_t &key, unsigned int &offset, int &length) {
//    std::cout<<"[info]is finding "<<key<<std::endl;
    SSTheader *header = cache.header;
    // 检查范围
    if (key < header->key_min || key > header->key_max) {
        return false;
    }
    //检查BF  BF没有一定没有 BF有实际没有只会多花时间
    if (!cache.bloomFilter->contains(key)) {
        return false;
    }

    //二分查找 可能找不到
    uint64_t l = 0;
    uint64_t r = header->length - 1;
    uint64_t m;
//    std::cout << key << std::endl;
    while (l <= r) {
        m = (l + r) / 2;
//        std::cout << m << " ";
        if (cache.indexList[m].key == key) {
            //找到就设置相应信息 如果在末尾不好算长度 用特殊标记
            offset = cache.indexList[m].offset;
            if (m < header->length - 1) {//不是最后一个
                length = cache.indexList[m + 1].offset - offset;
            }else{
                length = -1;
            }
            return true;
        }
        if (cache.indexList[m].key < key) {
            l = m + 1;
        } else {
            r = m - 1;
        }
    }
    return false;
}

//在指定位置读指定长度个字符 返回字符串
//想到的方法只有seekg+read    char[]转string
value_t KVStore::getValueFromSST(const timeStamp_t &t, const unsigned int &offset, const int &length) {
//    std::cout<<"[info]is finding in "<<timeStamp<<" offset/length:"<<offset<<" "<<length<<std::endl;
    std::ifstream f("../data/" + std::to_string(t), std::ios::binary);
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