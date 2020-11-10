## TIANCHI Tair Contest 性能挑战赛

### 1. Introduction

[天池第二届数据库性能挑战赛](https://tianchi.aliyun.com/competition/entrance/531820/introduction)

[赛题描述](https://code.aliyun.com/db_contest_2nd/tair-contest)

### 2. [Season1/初赛/第一赛季 ](https://github.com/JYLeeLYJ/tair_contest_code/tree/season1)

**RANK:** 18/2170 | 
**SCORE:** 82230

* Hash索引:开放定址法+线性探测(baseline)
* 利用atomic实现lock-free代替对索引分桶上锁
* simple bloom filter减少set阶段查找
* SIMD多字节key_cmp_eq
* inline定长版本__hash_bytes代替```std::hash<std::string>{}();```内部的libstdc++实现
* 按线程分区写入,关键信息尽可能thread_local化减少cache-shared
* 按cacheline size对齐
* __builtin_expect (likely/unlikely)

回想遗漏的优化点：
* search阶段的pmemIO平均可以减少到0次(接近),从而代替不太必要的bloom filter，并节省读占用的XPBuffer空间
* 挤出少量剩余内存做lru cache应该能有效较少Get阶段的pmemIO,降低latency

### 3. Season2/复赛/第二赛季

**RANK:** 12/106 | 
**SCORE:** 57651

* Lock-free hash 索引
* value数据区按128B对齐分块 , allocator按128/256 Bytes粒度分配
* 数据头alignas (64B)
* 分区写k-v,thread-local allocate/recollect
* thread-local LRU 缓存读热点数据, 减少get search阶段的pmem IO
* memcpy_avx

实际Set阶段的写带宽约 4GB/s

---

### PS

多线程程序基本优化顺序思路：
* 先尽量降低锁的粒度或者lock-free ,减少阻塞，减少cache的冲突，以求将并发拉满
* 剩下再思考单线程的性能优化，分析程序对应memory-bound/compute-bound , 针对相应的瓶颈一步步优化,避免过早优化非瓶颈代码，无法有效提高性能

实际工程中精确的profiling工具可以极大提高性能分析的效率，比赛阶段纯靠日志来验证需要一定的经验基础 :(
