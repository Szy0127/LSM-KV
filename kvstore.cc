#include "kvstore.h"
#include <string>

KVStore::KVStore(const std::string &dir) : KVStoreAPI(dir), length(0), size(0) {
    memTable = new SkipList;
}

KVStore::~KVStore() {
    delete memTable;
}

/**
 * Insert/Update the key-value pair.
 * No return values for simplicity.
 */
void KVStore::put(uint64_t key, const std::string &s) {
    memTable->put(key, s);
    length++;
    size += KEY_SIZE + OFFSET_SIZE + s.length();

    if (size >= MAX_SSTABLE_SIZE) {
        memCompaction();
    }
}

/**
 * Returns the (string) value of the given key.
 * An empty string indicates not found.
 */
std::string KVStore::get(uint64_t key) {
    return memTable->get(key);
}

/**
 * Delete the given key-value pair if it exists.
 * Returns false iff the key is not found.
 */
bool KVStore::del(uint64_t key) {
    return memTable->del(key);
}

/**
 * This resets the kvstore. All key-value pairs should be removed,
 * including memtable and all sstables files.
 */
void KVStore::reset() {
    memTable->reset();
}

/**
 * Return a list including all the key-value pair between key1 and key2.
 * keys in the list should be in an ascending order.
 * An empty string indicates not found.
 */
void KVStore::scan(uint64_t key1, uint64_t key2, std::list<std::pair<uint64_t, std::string> > &list) {
    memTable->scan(key1, key2, list);
}

void KVStore::memCompaction() {
//    std::cout << length << " " << size << std::endl;
    BloomFilter bloomFilter(length);
    std::list<std::pair<uint64_t, std::string>> list;
    memTable->getList(list);
    for (auto kv: list) {
//        std::cout << kv.first << " " << kv.second << std::endl;
        bloomFilter.insert(kv.first);
    }

}