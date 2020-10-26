#ifndef TAIR_CONTEST_KV_CONTEST_NVM_ENGINE_H_
#define TAIR_CONTEST_KV_CONTEST_NVM_ENGINE_H_

#include <atomic>
#include <climits>
#include <cstddef>
#include <cstdlib>
#include <iostream>
#include <atomic>

#include "include/db.hpp"
#include "include/kvfile.hpp"
#include "include/hash_index.hpp"
#include "include/allocator.hpp"
#include "include/open_address_hash_index.hpp"

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

    static constexpr size_t META_SIZE = 1_KB;

    #ifdef LOCAL_TEST
    static constexpr size_t DRAN_SIZE = 256_MB;
    static constexpr size_t NVM_SIZE = 64_MB ;
    static constexpr size_t KEY_AREA = 8_MB;
    static constexpr size_t INDEX_AREA = 8_MB;
    static constexpr size_t VALUE_AREA = NVM_SIZE - META_SIZE - KEY_AREA - INDEX_AREA ;
    #else
    static constexpr size_t DRAM_SIZE = 8_GB;
    static constexpr size_t NVM_SIZE = 64_GB ;
    static constexpr size_t KEY_AREA = 6_GB ;
    static constexpr size_t INDEX_AREA = 6_GB;
    static constexpr size_t VALUE_AREA = NVM_SIZE - META_SIZE - KEY_AREA - INDEX_AREA ;
    static_assert(VALUE_AREA > 50_GB , "" );
    #endif

    static constexpr size_t N_KEY = KEY_AREA / sizeof(head_info) ;
    static constexpr size_t N_VALUE = VALUE_AREA / sizeof(value_block);
    static constexpr size_t N_BLOCK_INDEX = INDEX_AREA / sizeof(block_index);
    static constexpr size_t N_IDINFO = N_KEY;

    static constexpr size_t THREAD_CNT = 16;
    static constexpr size_t BUCKET_CNT = THREAD_CNT;
    static constexpr size_t HASH_SIZE = N_KEY /2;

private:

    struct alignas(CACHELINE_SIZE) bucket_info{
        uint32_t hash_index_info_seq{};
        uint32_t key_seq{};
        uint32_t block_index_seq{};

        std::vector<uint32_t> free_index_blocks{};
    };

private:

    void recovery();
    head_info* search(const Slice & key , uint64_t hash) ;
    Status update(const Slice & value , uint64_t hash , head_info * head , uint32_t bucket_id);
    Status append(const Slice & key , const Slice & value , uint64_t hash , uint32_t bucket_id);

    std::pair<uint32_t , block_index> alloc_value_blocks(uint32_t bucket_id , uint32_t len);
    void recollect_value_blocks(uint32_t bucket_id , uint32_t index_block_id , uint32_t len);
    void write_value(const Slice & value  ,uint32_t index_block_id ,block_index & indics );
    void read_value(std::string & value , head_info * head);

    uint32_t get_bucket_id(){
        return thread_seq ++ % BUCKET_CNT;
    }

    uint32_t new_hash_index_info(uint32_t bucket_id){
        constexpr auto n_index_per_bk = N_IDINFO / BUCKET_CNT;
        return (bucket_infos[bucket_id].hash_index_info_seq ++) + bucket_id * n_index_per_bk;
    }

    uint32_t new_key_info(uint32_t bucket_id){
        constexpr auto n_key_per_bk = N_KEY / BUCKET_CNT;
        auto seq = bucket_infos[bucket_id].key_seq ++;
        return seq < n_key_per_bk ? seq + bucket_id * n_key_per_bk : index.null_id;
    }

private:

    kv_file_info<N_KEY , N_BLOCK_INDEX , N_VALUE> file;
    std::atomic<uint32_t> thread_seq{0};

    alignas(CACHELINE_SIZE)
    std::array<bucket_info , BUCKET_CNT> bucket_infos;
    alignas(CACHELINE_SIZE)
    std::array<value_block_allocator<N_VALUE / BUCKET_CNT> , BUCKET_CNT> allocator{};

    // hash_index<HASH_SIZE , N_IDINFO> index;
    open_address_hash<N_KEY * 2> index;

    static_assert(sizeof(bucket_info) == 64 , "");
    static_assert(sizeof(bucket_infos) == 1_KB , "");

};

#endif
