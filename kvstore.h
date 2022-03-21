#pragma once

#include "kvstore_api.h"
#include "SkipList.h"
#include "BloomFilter.h"
class KVStore : public KVStoreAPI {
	// You can add your implementation here
public:
    static const int KEY_SIZE = 8;
    static const int OFFSET_SIZE = 4;
    static const int MAX_SSTABLE_SIZE = 2*1024*1024;
private:
    SkipList *memTable;
    int length;//内存中元素个数
    int size;//key+offset+s.length

    void memCompaction();
public:
	KVStore(const std::string &dir);

	~KVStore();

	void put(uint64_t key, const std::string &s) override;

	std::string get(uint64_t key) override;

	bool del(uint64_t key) override;

	void reset() override;

	void scan(uint64_t key1, uint64_t key2, std::list<std::pair<uint64_t, std::string> > &list) override;
};
