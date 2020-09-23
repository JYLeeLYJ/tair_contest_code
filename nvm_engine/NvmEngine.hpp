#ifndef TAIR_CONTEST_KV_CONTEST_NVM_ENGINE_H_
#define TAIR_CONTEST_KV_CONTEST_NVM_ENGINE_H_

#include <atomic>
#include <climits>
#include <cstddef>
#include <cstdlib>
#include <iostream>
#include <atomic>

#include "include/db.hpp"
#include "include/boolean_filter.hpp"

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
    std::pair<uint32_t , uint32_t> search(const Slice & key , uint64_t hash);
    bool append(const Slice & key , const Slice & value , uint32_t i);
    uint32_t allocated_index(uint32_t hash);


private:

    struct entry_t {
        char key[16];
        char value[80];
    };

    #ifdef LOCAL_TEST
    static constexpr size_t NVM_SIZE = 2*1024*1024*sizeof(entry_t);
    static constexpr size_t DRAM_SIZE = 16 * 1024 * 1024;       //16M
    static constexpr size_t FILTER_SIZE = 1 * 1024 * 1024;
    #else
    static constexpr size_t NVM_SIZE = 79456894976;
    static constexpr size_t DRAM_SIZE = 4200000000;
    static constexpr size_t FILTER_SIZE = 360ULL * 1024 * 1024 * 8;
    #endif


    //load factor ~ 0.73
    static constexpr uint32_t ENTRY_MAX = NVM_SIZE / sizeof(entry_t);       //74G / 80 entries
    static constexpr uint32_t BUCKET_MAX = DRAM_SIZE / sizeof(std::atomic<uint32_t>);    //1G buckets

    entry_t *entry{};
    std::atomic<uint32_t> *bucket{};
    std::atomic<uint32_t> entry_cnt {1};

    //256M
    bitmap_filter<FILTER_SIZE> bitset{};
};

#endif
