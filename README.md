## TIANCHI Tair Contest 性能挑战赛

### 1. Introduction

[天池第二届数据库性能挑战赛](https://tianchi.aliyun.com/competition/entrance/531820/introduction)
[赛题描述](https://code.aliyun.com/db_contest_2nd/tair-contest)

### 2. Season1/初赛/第一赛季 

**RANK:** 18/2170 
**SCORE:** 82230

* 线性开放定址法索引(baseline)
* atomic代替索引分桶上锁
* simple bloom filter减少set查找
* SIMD多字节key_cmp_eq
* inline定长版本__hash_bytes实现
* 按线程分桶写入,thread_local减少cache-shared
* 按cacheline size对齐
* __builtin_expect (likely/unlikely)