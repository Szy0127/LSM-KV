SSTable：限制2MB 2 *1024 *1024=2097152 bytes

- 32bytes头

  - 时间戳8bytes
  - 数量 long long    8bytes
  - 最小值 8bytes
  - 最大值8bytes

- 10240字节的Bloom Filter (m=10240*8=81920)

- key+offset 每组12bytes

  - key: 8bytes

  - offset:4bytes

    文件大小限制为2097152bytes 可以用int存下

- value:不定长的字符串 多少字符就是多少bytes

设value平均长x字节

数量为$\frac{2097152-32-10240}{12+x}=\frac{2086880}{12+x}$

这里x应该能根据应用的场景事先确定

测试中最大的x是1024 * 64 得到数量为31.8个

最小若取1 得到数量为160529个



跳表中设置p=0.25

最大高度为$log_4(160529)=8.6$

布隆过滤器最小误报率为$k=ln2 \frac{m}{n}$ 

在保存与读取的时候都需要重构一个布隆过滤器 所以不妨让 k也是变量



内存中的跳表每次插入时时刻记录当前大小

记录数量（为了构造布隆过滤器）

与大小（到2m就写入文件）



memTable

SSTable 实现

缓存 实现



缓存存储的内容是除了value之外的所有部分

缓存：

- header 32bytes 直接存结构体
  - 时间戳 8
  - 数量 8
  - 最小键 8
  - 最大键 8
- BloomFilter 10240bytes 直接存类
- IndexList 16*length   用数组的两个好处 1 方便直接存  连续简单  2 方便二分查找直接索引
  - key 8
  - offset 4



待完成：

1. 涉及sstable的scan

2. 如果没有缓存 需要去所有文件里找
3. 文件合并 归并排序
4. 最后一次处理delete





同一个key的数据 可能由于不同时刻的修改  被存入了不同的sstable中

但实际上面向用户的get应该只给出一个结果

所以在get的时候应该按照时间戳由大到小的顺序去查询



由于启动时会加载所有的信息到内存

运行时在保存的同时也会加载到内存

所以只需要关注内存中的保存机制



缓存是有先后顺序的   在查找时应该优先查询后加入的缓存

还是得使用map 需要key(时间戳)作为排序的标志  key越大越前面

如果用字符串排序 10在2前 不行 自定义查找算法插入的时候效率太慢



scan操作 禁止一个个get

对于sstable的索引区和跳表 都可以把连续一段的内容取出来

取出后直接放入堆中

第一个问题 堆排序排序的是key  但是kv的映射关系在出了跳表或sstable就未知了 所以堆要同时保存value的信息 所以需要自定义一个排序函数

第二个问题 如果不同的sstable中存在两个不同的key  应该以时间戳大的为准 但是堆本身并不会进行覆盖

解决方法 堆外套wrapper：

第一种 时间戳由大到小遍历 sstable  如果key已经存在于队列 则不进行push

如果每次遍历队列 复杂度太高 不如新加一个hashtable

第二种 时间戳由小到大遍历sstable key如果存在 则覆盖value

选择第一种





待完成：

compation
