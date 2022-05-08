#include "kvstore.h"
#include <memory>
#include <string>
#include <cassert>


std::string KVStore::SUFFIX = ".sst";

KVStore::KVStore(const std::string &dir) : KVStoreAPI(dir), dataPath(dir), compactionLevel(0), timeCompaction(0),
                                           fileSuffix(0), timeStamp(1) {
    if(!utils::dirExists(dir)){
        utils::mkdir(dir.c_str());
    }
    if (dataPath.back() != '/') {
        dataPath.push_back('/');
    }
    memTable = new Mymap;
    //测试的时候每次是从头开始的 需要把本地的文件删干净 或者不进行读入的操作 或者修改测试的代码 加reset
    loadSST();
}

void KVStore::loadSST() {
    //把所有sstable的索引部分都读入缓存 并且找到最后一个时间戳
    std::vector<std::string> levelDirs;
    std::vector<std::string> sstables;
    std::string levelPath;
    utils::scanDir(dataPath, levelDirs);
    int level_count = 0;
    for (auto &level: levelDirs) {
        if (cacheAllSST.size() < level_count + 1) {
            cacheAllSST.emplace_back(CacheLevelTime());
//            cacheKey.emplace_back(CacheLevelKey());
        }
        levelPath = dataPath + level + "/";
        sstables.clear();
        utils::scanDir(levelPath, sstables);
        for (auto &sstable: sstables) {
            std::string name = sstable.substr(0, sstable.find(SUFFIX));
            timeStamp_t t = std::stoull(name.substr(0, name.find('_')));//这个分隔符需要和memcompation里保持一致
            timeStamp_t fs = std::stoull(name.substr(name.find('_') + 1));
            if (t > timeStamp) {
                timeStamp = t;
            }
            if (fs > fileSuffix) {
                fileSuffix = fs;
            }
            std::shared_ptr<CacheSST> cache = getCacheFromSST(levelPath + sstable);
            cacheAllSST[level_count].insert(std::make_pair(t, cache));
//            cacheKey[level_count].insert(std::make_pair(cache->header->key_min, cache));
        }
        level_count++;
    }
    cacheAllSST.emplace_back(CacheLevelTime());
//    cacheKey.emplace_back(CacheLevelKey());
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
//        std::cout<<key<<"begin compaction"<<std::endl;
        memCompaction();
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
    unsigned int length;
    bool find;
//    std::cout << "finding"<<key << std::endl;
    for (auto const &cacheLine: cacheAllSST) {
        for (auto const &cache: cacheLine) {
//            std::cout << cache.second->header->timeStamp << " ";
            //在找到的同时拿到位置与大小信息
//        std::cout<<cache.first<<" "<<cache.second.header->timeStamp<<std::endl;
            bool last = false;


            find = getValueInfo(*cache.second, key, offset, length, last);
            if (find) {
                //根据文件名 位置 大小 直接读出信息
                std::ifstream f(cache.second->path);
                f.seekg(offset);
                std::string res;
                if (!last) {
                    res = readStringFromFileByLength(f, length);
                } else {
                    res = readStringFromFileByLength(f, 0, true);
                }
                f.close();
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
        return false;//跳表和sstable(缓存)中都没找到
    }
    if (memTable->get(key) != NOTFOUND) {
        memTable->del(key);//在跳表里 可以直接删
        return true;
    }
    put(key, DELETED);
    return true;
}

/**
 * This resets the kvstore. All key-value pairs should be removed,
 * including memtable and all sstables files.
 */
void KVStore::reset() {
    timeStamp = 0;
    fileSuffix = 0;
    compactionLevel = 0;
    timeCompaction = 0;
    memTable->reset();
    cacheAllSST.clear();
//    cacheKey.clear();
    std::vector<std::string> levelDir, files;
    std::string levelPath;
    utils::scanDir(dataPath, levelDir);
    for (auto &level: levelDir) {
        levelPath = dataPath + level + '/';
//        std::cout<<levelPath<<std::endl;
        files.clear();
        utils::scanDir(levelPath, files);
        for (auto &file: files) {
//            std::cout<<file<<" ";
            utils::rmfile((levelPath + file).c_str());
        }
        utils::rmdir(levelPath.c_str());
    }
    utils::rmdir(dataPath.c_str());
    cacheAllSST.emplace_back(CacheLevelTime());
//    cacheKey.emplace_back(CacheLevelKey());
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

    for (auto const &cacheLine: cacheAllSST) {
        {
            for (auto const &cache: cacheLine) {
                if (key1 > cache.second->header->key_max || key2 < cache.second->header->key_min) {
                    continue;
                }
                //第一个大于等于key1的key的位置
                uint64_t begin = key1 <= cache.second->header->key_min ? 0 : binarySearchScan(cache.second->indexList,
                                                                                              cache.second->header->length,
                                                                                              key1, true);
                uint64_t end =
                        key2 >= cache.second->header->key_max ? cache.second->header->length - 1 : binarySearchScan(
                                cache.second->indexList, cache.second->header->length, key2, false);
                scanInSST(cache.second->path, cache.second->indexList, begin, end,
                          end == cache.second->header->length - 1,
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
    if (memTable->getSize() <= 0) {
        return;
    }
    fileSuffix++;
    std::list<std::pair<uint64_t, std::string>> list;
    memTable->getList(list);
// 头部 时间戳 数量 最大值 最小值 各8bytes
    timeStamp_t t = timeCompaction ? timeCompaction : ++timeStamp;
    timeCompaction = 0;
    uint64_t length;
    key_t key_min, key_max;

    length = memTable->getLength();
    //跳表本身是有序排列的  所以不需要动态维护key的范围 stl的list是双向链表 所以back应该不需要遍历
    key_min = list.front().first;
    key_max = list.back().first;


    std::string path = dataPath + "level-" + std::to_string(compactionLevel) + "/";
    if (!utils::dirExists(path)) {
        utils::mkdir(path.c_str());
    }
    path.append(std::to_string(t).append("_" + std::to_string(fileSuffix)).append(SUFFIX));
    std::ofstream f(path, std::ios::binary);



//    std::cout << "[debug]writeSST:" << path << " " << length << " " << key_min << " " << key_max << std::endl;

    auto *header = new SSTheader(t, length, key_min, key_max);
    f.write((char *) header, HEADER_SIZE);


    // 布隆过滤器 10240bytes
    auto *bloomFilter = new BloomFilter(length);
    for (auto const &kv: list) {
//        std::cout << kv.first << " " << kv.second << std::endl;
        bloomFilter->insert(kv.first);
    }
    f.write((char *) bloomFilter, sizeof(BloomFilter));


//     key_offset 共(8+4)*length bytes offset是value的绝对定位
    auto *indexList = new Index[length];
    unsigned int value_offset = HEADER_SIZE + BF_SIZE + sizeof(Index) * length;
    int i = 0;
    for (auto const &kv: list) {
        indexList[i].key = kv.first;
        indexList[i].offset = value_offset;
        i++;
        value_offset += kv.second.length();
    }
    f.write((char *) indexList, static_cast<std::streamsize>(sizeof(Index) * length));


    std::shared_ptr<CacheSST> cache(new CacheSST(path, header, bloomFilter, indexList));

    cacheAllSST[compactionLevel].insert(std::make_pair(t, cache));
//    cacheKey[compactionLevel].insert(std::make_pair(key_min, cache));
    //所有value连续存
    for (auto const &kv: list) {
//        f.write((char*)&kv.second,kv.second.length());
// 这里用f.write会乱 用流输出就正常
        f << kv.second;
    }


    f.close();
//    read(path);
    memTable->reset();

//    unsigned int offset;
//    unsigned int l;
//    bool last;
//    bool find = getValueInfo(*cache, 32522, offset, l, last);
//    if (find) {
//        std::ifstream f(path);
//        f.seekg(offset);
//        value_t res = readStringFromFileByLength(f, l, last);
//        std::cout << "[debug]compaction:" << path << " " << res.length() << std::endl;
//    }

    //compactionLevel==0 用户插入  需要检查
    // !=0 合并时借助同一个函数生成sstable 不需要检查
    if (compactionLevel == 0) {
        compaction(0);
    }
}


bool KVStore::isIntersect(key_t &l1, key_t &r1, key_t &l2, key_t &r2) {
    if (r1 < l2 || r2 < l1) {
        return false;
    }
    return true;
}

value_t KVStore::readStringFromFileByLength(std::ifstream &f, unsigned int length, bool last) {
//    if (length < 0) {
    if (last) {
        std::string v;
        f >> v;
        return v;
    }
    char *c = new char[length + 1];
    f.read(c, length);
    c[length] = '\0';
    std::string v(c);
    delete[] c;
    return v;

}

/*
 *
 * 准备好归并排序需要的所有局部变量
 * 开始归并排序 当一个文件结束后更新变量 继续
 * 当一层所有文件合并完后  继续更新另一层的文件
 */
void KVStore::mergeSort2Ways(CacheLevelKey &cacheLevelKeyUp, CacheLevelKey &cacheLevelKeyDown, bool last_level) {
    auto itup = cacheLevelKeyUp.begin();
    auto itdown = cacheLevelKeyDown.begin();


    std::shared_ptr<CacheSST> cacheUp = itup->second;
    std::shared_ptr<CacheSST> cacheDown = itdown->second;

    timeStamp_t timeUp = 0;
    timeStamp_t timeDown = 0;//便于在下层没有文件需要合并时选择时间戳
    std::ifstream fup;
    std::ifstream fdown;

    uint64_t sizeUp = 0;//
    uint64_t sizeDown = 0;// 如果下层没有的话 j==sizeDown 会跳过归并

    Index *indexListUp;// = cacheUp->indexList;
    Index *indexListDown;// = cacheDown->indexList;

    updateIterInfo(cacheUp, timeUp, fup, sizeUp, indexListUp);
    if (!cacheLevelKeyDown.empty()) {//如果下层没有需要合并的实际上只要移动sstable就行 但是utils里没有给不同平台下的移动文件函数
        updateIterInfo(cacheDown, timeDown, fdown, sizeDown, indexListDown);
    }

    key_t keyUp = 0;
    key_t keyDown = 0;
    value_t value;
    int i = 0;
    int j = 0;

//    std::cout<<"compact"<<cacheUp->path<<" "<<cacheDown->path<<"to"<<timeCompaction<<std::endl;


    //两路归并
    while (i < sizeUp && j < sizeDown) {
        keyUp = indexListUp[i].key;
        keyDown = indexListDown[j].key;
        if (keyUp <= keyDown) {
            if (i < sizeUp - 1) {
                value = readStringFromFileByLength(fup, indexListUp[i + 1].offset - indexListUp[i].offset);
            } else {
                value = readStringFromFileByLength(fup, 0, true);
            }
            if (!last_level || value != DELETED) {//最后一层不需要保留delete标记
                timeCompaction = timeUp > timeCompaction ? timeUp : timeCompaction;//每次操作都更新 防止剩余情况
                put(keyUp, value);
            }

            i++;
            if (keyUp == keyDown) {//相等的时候直接忽略时间戳大的 如果一前一后put会在临界点引发问题
//                readStringFromFileByLength(fdown, length);// 用这个方法一模一样读出来 会指针错 debug也看不出错那
                if (j < sizeDown - 1) {//如果没到末尾需要跳过这个kv 如果到了就不用管了
                    fdown.seekg(indexListDown[j + 1].offset);
                }
                j++;
            }
        } else {
            if (j < sizeDown - 1) {
                value = readStringFromFileByLength(fdown, indexListDown[j + 1].offset - indexListDown[j].offset);
            } else {
                value = readStringFromFileByLength(fdown, 0, true);
            }
            if (!last_level || value != DELETED) {//最后一层不需要保留delete标记
                timeCompaction = timeDown > timeCompaction ? timeDown : timeCompaction;
                put(keyDown, value);
            }
            j++;
        }
        if (i == sizeUp) {
            itup++;
            if (itup == cacheLevelKeyUp.end()) {
                break;
            }
            i = 0;
            cacheUp = itup->second;
            updateIterInfo(cacheUp, timeUp, fup, sizeUp, indexListUp);
        }
        if (j == sizeDown) {
            itdown++;
            if (itdown == cacheLevelKeyDown.end()) {
                break;
            }
            j = 0;
            cacheDown = itdown->second;
            updateIterInfo(cacheDown, timeDown, fdown, sizeDown, indexListDown);
        }
    }
    while (itup != cacheLevelKeyUp.end()) {//如果一开始下层就没有会直接进入这里
        for (; i < sizeUp - 1; i++) {
            keyUp = indexListUp[i].key;
            value = readStringFromFileByLength(fup, indexListUp[i + 1].offset - indexListUp[i].offset);
            if (!last_level || value != DELETED) {//最后一层不需要保留delete标记
                timeCompaction = timeUp > timeCompaction ? timeUp : timeCompaction;
                put(keyUp, value);
            }
        }
        keyUp = indexListUp[sizeUp - 1].key;
        value = readStringFromFileByLength(fup, 0, true);
        if (!last_level || value != DELETED) {//最后一层不需要保留delete标记
            timeCompaction = timeUp > timeCompaction ? timeUp : timeCompaction;
            put(keyUp, value);
        }
        itup++;
        if (itup == cacheLevelKeyUp.end()) {
            break;
        }
        i = 0;
        cacheUp = itup->second;
        updateIterInfo(cacheUp, timeUp, fup, sizeUp, indexListUp);
    }
    while (itdown != cacheLevelKeyDown.end()) {
        for (; j < sizeDown - 1; j++) {
            keyDown = indexListDown[j].key;
            value = readStringFromFileByLength(fdown, indexListDown[j + 1].offset - indexListDown[j].offset);
            if (!last_level || value != DELETED) {//最后一层不需要保留delete标记
                timeCompaction = timeDown > timeCompaction ? timeDown : timeCompaction;
                put(keyDown, value);
            }
        }
        keyDown = indexListDown[sizeDown - 1].key;
        value = readStringFromFileByLength(fdown, 0, true);
        if (!last_level || value != DELETED) {//最后一层不需要保留delete标记
            timeCompaction = timeDown > timeCompaction ? timeDown : timeCompaction;
            put(keyDown, value);
        }
        itdown++;
        if (itdown == cacheLevelKeyDown.end()) {
            break;
        }
        j = 0;
        cacheDown = itdown->second;
        updateIterInfo(cacheDown, timeDown, fdown, sizeDown, indexListDown);
    }
    fup.close();
    fdown.close();
}

void KVStore::mergeSort4Ways(CacheLevelKey &cacheLevelKeyUp, CacheLevelKey &cacheLevelKeyDown, bool last_level) {

    //用vector是因为对于map容器来说没法索引 想拿3个只能另外计数 但计数变量的作用域扩大了
    std::vector<std::shared_ptr<CacheSST>> caches;
    std::vector<uint64_t> size(4, 0);
    std::vector<uint64_t> index(4, 0);
    std::vector<Index *> indexList(4, nullptr);
//    std::vector<std::ifstream> f;
    std::ifstream f[4]{};
    std::vector<timeStamp_t> time(4, 0);

    for (auto const &cache: cacheLevelKeyUp) {
        caches.emplace_back(cache.second);
//        f.emplace_back(std::ifstream(cache.second->path));
    }
    int way = 3;
    auto it = cacheLevelKeyDown.begin();
    if (!cacheLevelKeyDown.empty()) {
        caches.emplace_back(it->second);
//        f.emplace_back(std::ifstream(it->second->path));
        way = 4;
    }

    //用统一的函数从cache中拿到归并时需要的相关变量
    for (int i = 0; i < way; i++) {
        updateIterInfo(caches[i], time[i], f[i], size[i], indexList[i]);
    }

    key_t key;
    value_t value;

    while (true) {
        key_t keyMin = UINT64_MAX;
        int loc = -1;
        for (int i = 0; i < way; i++) {//找最小的一个key
            if (index[i] >= size[i]) {//这一路结束了
                continue;
            }
            key = indexList[i][index[i]].key;
            if (key <= keyMin) {
                bool change = true;
                if (key == keyMin) {//相同的时候只需要保留时间戳大的
                    int tmp = time[loc] <= time[i] ? loc : i;
                    change = tmp == loc;
                    if (index[tmp] < size[tmp] - 1) {
                        f[tmp].seekg(indexList[tmp][index[tmp] + 1].offset);
                    }
                    index[tmp]++;
                }
                keyMin = key;
                if (change) {
                    loc = i;
                }
            }
        }
        if (loc == -1) {//4路都结束了
            break;
        }
        if (index[loc] < size[loc] - 1) {
            value = readStringFromFileByLength(f[loc], indexList[loc][index[loc] + 1].offset -
                                                       indexList[loc][index[loc]].offset);
        } else {
            value = readStringFromFileByLength(f[loc], 0, true);
        }
        if (!last_level || value != DELETED) {//最后一层不需要保留delete标记
            timeCompaction = time[loc] > timeCompaction ? time[loc] : timeCompaction;//每次操作都更新 防止剩余情况
            put(keyMin, value);
        }
        index[loc]++;
        if (way == 4 && index[3] == size[3]) {//第1层某一个sstable读完了 接着下一个
            it++;
            if (it == cacheLevelKeyDown.end()) {
                way = 3;
                continue;
            }
            index[3] = 0;
            caches[3] = it->second;
            updateIterInfo(caches[3], time[3], f[3], size[3], indexList[3]);
        }
    }

    for (int i = 0; i < way; i++) {
        f[i].close();
    }
}

/*
 * 检查是否需要合并
 * 如果是最后一层向下 新开一层
 * 找到上层时间戳最小的若干个 并按key排序
 * 找到下层与上层key相交的若干个 并按key排序
 * 归并排序
 * 把此轮剩余部分也放入文件中
 * 删除两层中被合并的文件及缓存
 * 继续向下合并
 *
 *
 */
void KVStore::compaction(int level) {//level满了 向level+1合并
    int max = maxLevelSize(level);
    if (cacheAllSST[level].size() <= max) {
        return;//不需要compaction  结束递归
    }
    //检查不相交 有迭代问题遗留
//    if (level > 0 && cacheAllSST.size() >= 1 && cacheAllSST[level].size() >= 1) {
//
//        std::map<key_t,std::pair<key_t,std::string>> tmp;
//        for(auto  c:cacheAllSST[level]){
//            tmp.insert(std::make_pair(c.second->header->key_min,
//                                      std::make_pair(c.second->header->key_max, c.second->path)));
//        }
//
//        std::vector<std::pair<key_t,key_t>> t;
//        std::vector<std::string> pa;
//        for(auto p:tmp){
//            t.emplace_back(std::make_pair(p.first, p.second.first));
//            pa.emplace_back(p.second.second);
//        }
//        for(int i = 1; i < t.size();i++){
//            if(isIntersect(t[i].first, t[i].second, t[i - 1].first, t[i - 1].second)){
//                std::cout<<level<<pa[i]<<" "<<pa[i-1]<<std::endl;
//                exit(0);
//            }
//        }
//    }


    if (cacheAllSST.size() <= level + 1) {//如果level是最后一层 需要向下再新建一层
        cacheAllSST.emplace_back(CacheLevelTime());
//        cacheKey.emplace_back(CacheLevelKey());
        std::string dirPath = dataPath + "level-" + std::to_string(level + 1);
        if (!utils::dirExists(dirPath)) {
            utils::mkdir(dirPath.c_str());
        }
    }



    //如果使用遍历每个上层sst判断重合的方法 会有一个反例
    //上层100-300 400-500  下层300-400 不会认为相交 故不compaction
    //但是在合并过程中  上层两个可能会写入一个sst 直接与下层没有合并的相交了
    key_t keyMin = UINT64_MAX;//记录上层区间
    key_t keyMax = 0;
    int count = 1;
    max = level > 0 ? max : 0;//第0层要全部向下合并
    CacheLevelKey cacheLevelKeyUp;
    for (auto const &cache: cacheAllSST[level]) {//取时间戳最小的若干个  也就是最后的
        if (count > max) {
            cacheLevelKeyUp.insert(std::make_pair(cache.second->header->key_min, cache.second));
            if(cache.second->header->key_min < keyMin){
                keyMin = cache.second->header->key_min;
            }
            if(cache.second->header->key_max > keyMax){
                keyMax = cache.second->header->key_max;
            }
        }
        count++;
    }

    CacheLevelKey cacheLevelKeyDown;
    for (auto const &cacheDown: cacheAllSST[level + 1]) {//找到下层与上层key有相交的部分进行合并
            auto headerDown = cacheDown.second->header;
            if (isIntersect(keyMin, keyMax, headerDown->key_min,headerDown->key_max)) {
                cacheLevelKeyDown.insert(std::make_pair(headerDown->key_min, cacheDown.second));
            }
    }

    compactionLevel = level + 1;//决定memcompation保存文件的位置
    bool last_level = (level == cacheAllSST.size() - 2);//最后一层删delete
    if (level > 0) {
        mergeSort2Ways(cacheLevelKeyUp, cacheLevelKeyDown, last_level);
    } else {
        mergeSort4Ways(cacheLevelKeyUp, cacheLevelKeyDown, last_level);
    }
    memCompaction();
    compactionLevel = 0;
    timeCompaction = 0;

    //这些多出来的要从原来的缓存和文件中删除 新加入的缓存会在memCompation中自动进行
    for (auto it = cacheLevelKeyUp.begin(); it != cacheLevelKeyUp.end(); it++) {
        removeSST(it, level);
    }
    for (auto it = cacheLevelKeyDown.begin(); it != cacheLevelKeyDown.end(); it++) {
        removeSST(it, level + 1);
    }
    compaction(level + 1);
}

void KVStore::removeSST(CacheLevelKey::iterator &cacheIt, int level) {
    std::string path = cacheIt->second->path;
//    std::cout<<"[debug]remove "<<path<<" "<<cacheIt->second->header->key_min<<" "<<cacheIt->second->header->key_max<<std::endl;
    //multimap直接调erase(key)会删掉所有内容  使用find找到第一个key 然后比对唯一的path
    auto itTime = cacheAllSST[level].find(cacheIt->second->header->timeStamp);
    while (itTime->second->path != path) {
        itTime++;
    }
    //同时把缓存和文件都删了
    cacheAllSST[level].erase(itTime);
    utils::rmfile(path.c_str());
}

std::shared_ptr<KVStore::CacheSST> KVStore::getCacheFromSST(const std::string &path) {
    std::ifstream f(path, std::ios::binary);

    auto *header = new SSTheader();
    f.read((char *) header, HEADER_SIZE);

//    std::cout << "read:" << header.length << " " << header.key_min << " " << header.key_max << std::endl;
    auto *bloomFilter = new BloomFilter();
    f.read((char *) bloomFilter, sizeof(BloomFilter));

    auto *indexList = new Index[header->length];
    f.read((char *) indexList, static_cast<std::streamsize>(sizeof(Index) * header->length));

//    caches[path] = Cache(header,bloomFilter,indexList);//这样会报错 需要默认构造函数 可能是复制过去？
    f.close();
    return std::make_shared<CacheSST>(path, header, bloomFilter, indexList);
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
        (indexList[m].key < key ? l : r) = m;
    }
    if (begin) {
        return r;
    }
    return l;
}

bool KVStore::getValueInfo(const CacheSST &cache, key_t key, unsigned int &offset, unsigned &length, bool &last) {
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
        last = true;
    }
    return true;
}


void
KVStore::scanInSST(const std::string &path, const Index *const indexList, const uint64_t &begin, const uint64_t &end,
                   bool last, KVheap &heap) {
    std::ifstream f(path, std::ios::binary);
    unsigned int offset = indexList[begin].offset;
    f.seekg(offset);
    unsigned int length;
    for (uint64_t i = begin; i < end; i++) {
        length = indexList[i + 1].offset - offset;
        std::string v = readStringFromFileByLength(f, length);
        if (v != DELETED) {
            heap.push(std::make_pair(indexList[i].key, v));
        }
        offset += length;
    }

    std::string v;
    if (!last) {
        v = readStringFromFileByLength(f, indexList[end + 1].offset - offset);
    } else {
        v = readStringFromFileByLength(f, 0, true);
    }
    heap.push(std::make_pair(indexList[end].key, v));
    f.close();
}

int KVStore::maxLevelSize(int level) {
    return 1 << (level + 1);
}

void KVStore::updateIterInfo(std::shared_ptr<CacheSST> &cache, timeStamp_t &t, std::ifstream &f, uint64_t &size,
                             Index *&indexList) {
    t = cache->header->timeStamp;
    size = cache->header->length;
    indexList = cache->indexList;
    f.close();
    f.clear();
    f.open(cache->path);
    f.seekg(indexList[0].offset);
}