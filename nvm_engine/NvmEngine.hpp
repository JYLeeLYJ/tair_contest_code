#ifndef TAIR_CONTEST_KV_CONTEST_NVM_ENGINE_H_
#define TAIR_CONTEST_KV_CONTEST_NVM_ENGINE_H_

#include <atomic>
#include <climits>
#include <cstddef>
#include <cstdlib>
#include <iostream>
#include <atomic>

#include "include/db.hpp"

class NvmEngine : DB {
public:
    /**
     * @param 
     * name: file in AEP(exist)
     * dbptr: pointer of db object
     *
     */
    static Status CreateOrOpen(const std::string &name, DB **dbptr);
    NvmEngine(const std::string &name);
    Status Get(const Slice &key, std::string *value);
    Status Set(const Slice &key, const Slice &value);
    ~NvmEngine();

private:
    std::pair<uint32_t , uint32_t> search(const Slice & key , uint32_t hash);
    bool append(const Slice & key , const Slice & value , uint32_t i);

private:

    struct entry_t {
        char key[16];
        char value[80];
    };

    #ifdef LOCAL_TEST
    static const size_t NVM_SIZE = 2*1024*1024*sizeof(entry_t);
    static const size_t DRAM_SIZE = 16 * 1024 * 1024;       //16M
    #else
    static const size_t NVM_SIZE = 79456894976;
    static const size_t DRAM_SIZE = 4200000000;
    #endif

    //load factor ~ 0.73
    static const uint32_t ENTRY_MAX = NVM_SIZE / sizeof(entry_t);       //74G / 80 entries
    static const uint32_t BUCKET_MAX = DRAM_SIZE / sizeof(std::atomic<uint32_t>);    //1G buckets
    // static const uint32_t BUCKET_PER_MUTEX = 30000000;
    // static const uint32_t MUTEX_CNT = BUCKET_MAX / BUCKET_PER_MUTEX + 1;    //140 mutex

    entry_t *entry{};
    std::atomic<uint32_t> *bucket{};
    std::atomic<uint32_t> entry_cnt {1};
    // std::mutex slot_mut[MUTEX_CNT];
};

#endif
