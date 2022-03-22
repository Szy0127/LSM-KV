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



待完成：

SSTable读进来的KV如何存储？

如何把读进来的SSTable进行查找？

二分查找算法