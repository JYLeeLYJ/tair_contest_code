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
    bool append(const Slice & key , const Slice & value , uint32_t i , uint64_t hash);

private:

    uint32_t allocate_index(uint32_t bucket_id){
        uint32_t index = ++ _entry_seq[bucket_id];
        return index < ENTRY_PER_BUCKET ? index + bucket_id * ENTRY_PER_BUCKET : 0;
    }
    bool is_valid_index(uint32_t i) {
        return i && i < BUCKET_MAX; 
    }
    uint32_t allocated_bucket_id(){
        return (_thread_seq ++ ) % WRITE_BUCKET;
    }

private:/* static definition */

    struct entry_t {
        char key[KEY_SIZE];
        char value[VALUE_SIZE];
    };

    struct key_t: std::array<char , KEY_SIZE>{};

    struct value_t : std::array<char , VALUE_SIZE>{};

    class  value_block_t : std::array<value_t , 4096 / VALUE_SIZE>{
        char align_bytes[4096 % VALUE_SIZE];
    };

    #ifdef LOCAL_TEST
    static constexpr size_t NVM_SIZE = 2*1024*1024*sizeof(entry_t);
    static constexpr size_t DRAM_SIZE = 16 * 1024 * 1024;       //16M
    static constexpr size_t FILTER_SIZE = 1 * 1024 * 1024;
    #else
    static constexpr size_t NVM_SIZE = 48ULL * 16 * 1024 * 1024 * sizeof(KEY_SIZE + VALUE_SIZE);
    static constexpr size_t DRAM_SIZE = 4200000000;
    static constexpr size_t FILTER_SIZE = 256ULL * 1024 * 1024 * 8;
    #endif

    //load factor ~ 0.73
    static constexpr uint32_t ENTRY_MAX = NVM_SIZE / sizeof(entry_t);       //74G / 80 entries
    static constexpr uint32_t BUCKET_MAX = DRAM_SIZE / sizeof(std::atomic<uint32_t>);    //1G buckets
    static constexpr uint32_t WRITE_BUCKET = 16;
    static constexpr uint32_t ENTRY_PER_BUCKET = ENTRY_MAX / WRITE_BUCKET;

private: /* data members */

    // entry_t *entry{};
    void * _base_ptr{};
    key_t * _key{};
    value_t * _value{};
    std::atomic<uint32_t> * _bucket{};
    
    alignas (CACHELINE_SIZE) 
    std::array<atomic_uint_align64_t, WRITE_BUCKET> _entry_seq{};
    std::atomic<uint32_t> _thread_seq{0};
    //256M
    bitmap_filter<FILTER_SIZE> _bitset{};

private: /* static asserts */

    static_assert(sizeof(value_block_t) == 4096 , "value_block_t must align as 4K.");
    static_assert(sizeof(_entry_seq) == CACHELINE_SIZE * WRITE_BUCKET , "seq should align as 64");
    static_assert(alignof(_entry_seq) == CACHELINE_SIZE  ,"seq should align as 64" );
};

#endif
