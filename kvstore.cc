#include "kvstore.h"
#include <string>


KVStore::KVStore(const std::string &dir) : KVStoreAPI(dir), length(0), size(HEADER_SIZE+BF_SIZE){
    BloomFilter::M = KVStore::BF_SIZE * 8;
    memTable = new SkipList;
    findTimeStamp();
}

KVStore::~KVStore() {
    for(auto cache:cacheList){
        delete cache.header;
        delete cache.indexList;
    }
    delete memTable;
}

/**
 * Insert/Update the key-value pair.
 * No return values for simplicity.
 */
void KVStore::put(key_t key, const value_t &s) {
    if (size + sizeof(Index) + s.length() >= MAX_SSTABLE_SIZE) {
        std::cout<<size;
        memCompaction();
        reset();
    }

    memTable->put(key, s);
    length++;
    size += sizeof(Index)+ s.length();
}

/**
 * Returns the (string) value of the given key.
 * An empty string indicates not found.
 */
std::string KVStore::get(key_t key) {
    return memTable->get(key);
}

/**
 * Delete the given key-value pair if it exists.
 * Returns false iff the key is not found.
 */
bool KVStore::del(key_t key) {
    return memTable->del(key);
}

/**
 * This resets the kvstore. All key-value pairs should be removed,
 * including memtable and all sstables files.
 */
void KVStore::reset() {
    size = HEADER_SIZE+BF_SIZE;
    length = 0;
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
    key_t key_min, key_max;
    memTable->getRange(key_min, key_max);
    std::string path = "../data/"+std::to_string(timeStamp);
    std::ofstream f(path, std::ios::binary);

    std::cout<<"write:"<<timeStamp<<" "<<length<<" "<<key_min<<" "<<key_max<<std::endl;
    SSTheader *header = new SSTheader(timeStamp, length, key_min, key_max);
    f.write((char *) header, HEADER_SIZE);

    std::list<std::pair<uint64_t, std::string>> list;
    memTable->getList(list);

    // 布隆过滤器 10240bytes
    BloomFilter bloomFilter(length);
    for (auto kv: list) {
//        std::cout << kv.first << " " << kv.second << std::endl;
        bloomFilter.insert(kv.first);
    }
    bloomFilter.store(f);

//     key_offset 共(8+4)*length bytes offset是value的绝对定位
    Index *indexList = new Index[length];
    unsigned int value_offset = HEADER_SIZE + BF_SIZE + sizeof(Index) * length;
    int i = 0;
    for (auto kv: list) {
        indexList[i].key = kv.first;
        indexList[i].offset = value_offset;
        i++;
        value_offset += kv.second.length();
    }
    f.write((char *) indexList, sizeof(Index) * length);
    cacheList.push_back(Cache(header,indexList,path));
    //所有value连续存
    for (auto kv: list) {
//        f.write((char*)&kv.second,kv.second.length());
// 这里用f.write会乱 用流输出就正常
        f << kv.second;
    }

    f.close();

    read(path);

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
    std::cout << wrong << std::endl;
}

void KVStore::read(std::string path) {

    std::ifstream f(path, std::ios::binary);

    SSTheader header;

    f.read((char *) &header, HEADER_SIZE);

    std::cout << "read:"<<header.length << " " << header.key_min << " " << header.key_max << std::endl;
    BloomFilter bloomFilter(header.length);
    bloomFilter.load(f);

    Index indexList[header.length];
    f.read((char *) indexList, sizeof(Index)* header.length);

    unsigned int value_length_max = 2048;
    unsigned int value_length;
    char *buffer = new char[value_length_max];
    for (int i = 1; i < header.length; i++) {
        value_length = indexList[i].offset - indexList[i - 1].offset;
        if (value_length > value_length_max) {
            value_length_max = value_length * 2;
            delete buffer;
            buffer = new char[value_length_max];
        }
        f.read(buffer,value_length);
        buffer[value_length] = '\0';
        std::string value(buffer);
//        std::cout<<indexList[i-1].key<<" "<<value<<std::endl;
    }
    std::string value;
    f>>value;
//    std::cout<<indexList[header.length-1].key<<" "<<value;

    f.close();

}

void KVStore::findTimeStamp() {
    timeStamp = 0;//遍历sstable 找到最后一个
}