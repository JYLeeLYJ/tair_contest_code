#include "NvmEngine.hpp"
#include "include/utils.hpp"
#include "include/logger.hpp"
#include "fmt/format.h"

#include <tuple>

#include <libpmem.h>
#include <sys/mman.h>

#include <cassert>

Status DB::CreateOrOpen(const std::string &name, DB **dbptr, FILE *log_file) {
    Logger::instance().set_file(log_file);
    return NvmEngine::CreateOrOpen(name, dbptr);
}

DB::~DB() {}

Status NvmEngine::CreateOrOpen(const std::string &name, DB **dbptr) {
    *dbptr = new NvmEngine(name);
    return Ok;
}

NvmEngine::NvmEngine(const std::string &name) {

    bucket = new std::atomic<uint32_t>[BUCKET_MAX]{};
    // memset(bucket, 0, sizeof(uint32_t) * BUCKET_MAX);

    if ((entry = (entry_t *)(pmem_map_file(name.c_str(),
                                          ENTRY_MAX * sizeof(entry_t),
                                          PMEM_FILE_CREATE, 0666, nullptr,
                                          nullptr))) == NULL) {
        perror("Pmem map file failed");
        exit(1);
    }
    Logger::instance().sync_log(name);
    Logger::instance().sync_log("*************************");
}

// static thread_local uint64_t hash_tm{0} , search_tm{0} , get_tm{0};

Status NvmEngine::Get(const Slice &key, std::string *value) {

    static bool flag{false};
    if(unlikely(flag == false)){
        flag = true;
        Logger::instance().log(fmt::format("*********** Start Get ***********"));
    }

    // static thread_local uint32_t cnt{0};
    // if(unlikely(cnt ++ % 5000000 == 0))
    //     Logger::instance().log(
    //         fmt::format("[Get]hash_tm = {} , search_tm = {} , assign_tm = {} " , hash_tm , search_tm , get_tm));

    uint64_t hash ;
    // {
    // time_elasped<std::chrono::nanoseconds> tm{hash_tm};
    // hash = std::hash<std::string>{}(key.to_string());
    hash = hash_bytes_16(key.data());
    // }

    uint32_t index {0} , bucket_i {std::numeric_limits<uint32_t>::max()};

    // {
    // time_elasped<std::chrono::nanoseconds> tm{search_tm};
    std::tie(index , bucket_i) = search(key , hash);
    // }

    if(likely(index)){
        // time_elasped<std::chrono::nanoseconds> tm{get_tm};
        value->assign(entry[index].value , 80);
        return Ok;
    }else{
        return NotFound;
    }
}

// static thread_local uint64_t append_tm {0} , search_test_tm{0} , write_tm{0} , setindex_tm{0} , update_tm{0};

Status NvmEngine::Set(const Slice &key, const Slice &value) {

    // static thread_local uint32_t cnt{0};
    // if(unlikely(cnt ++ % 5000000 == 0))
    //     Logger::instance().log(
    //         fmt::format("append_time = {} , search and test = {} , write and set = {} , setindex_tm = {} , hash = {} , update = {}" ,
    //         append_tm / 1000000, search_test_tm/1000000 , write_tm / 1000000 , setindex_tm / 1000000 , hash_tm/1000000 , update_tm / 1000000));


    uint64_t hash ;
    // {
    // time_elasped<std::chrono::nanoseconds> tm{hash_tm};
    // hash = std::hash<std::string>{}(key.to_string());
    hash = hash_bytes_16(key.data());
    // }
    uint32_t index {0} , bucket_id {static_cast<uint32_t>(hash % BUCKET_MAX)};

    // {
    //     time_elasped<std::chrono::nanoseconds> tm{search_test_tm};
    if(unlikely(bitset.test(hash % bitset.max_index))){
        std::tie(index , bucket_id) = search(key , hash);
    }

    // }

    //update
    if(unlikely(index)){
        // time_elasped<std::chrono::nanoseconds> tm{update_tm};
        memcpy_avx_80(entry[index].value , value.data());
        return Ok;
    }
    //insert
    else {
        // time_elasped<std::chrono::nanoseconds> tm{append_tm}; 
        return append(key , value , bucket_id , hash) ? Ok : OutOfMemory;
    }
}

std::pair<uint32_t,uint32_t> NvmEngine::search(const Slice & key , uint64_t hash){
    for (uint32_t i = 0 , bucket_id = hash % BUCKET_MAX; i < BUCKET_MAX ; i++) {
        //not found
        if(bucket[bucket_id] == 0)
            return {0 , bucket_id};
        
        uint32_t index = bucket[bucket_id];
        auto & ele = entry[index];

        //found
        if (fast_key_cmp_eq(ele.key, key.data()))
            return {index , i};
        
        ++bucket_id;
        bucket_id %= BUCKET_MAX;
    }

    //full bucket and not found
    return {0, std::numeric_limits<uint32_t>::max()};
}

bool NvmEngine::append(const Slice & key , const Slice & value, uint32_t i , uint64_t hash){
    static thread_local uint32_t write_bucket_id{allocated_bucket_id()};
    //allocated index 
    uint32_t index = allocate_index(write_bucket_id);
    //oom , full entry pool
    if(unlikely(!is_valid_index(index) || i == std::numeric_limits<uint32_t>::max()))
        return false;

    //write index
    uint32_t cnt = 0 ;
    {
    // time_elasped<std::chrono::nanoseconds> tm{setindex_tm};
    for (; cnt < BUCKET_MAX ; ++cnt ,++i , i %= BUCKET_MAX){
        if(bucket[i]) continue;

        uint32_t empty_val {0};
        if(bucket[i].compare_exchange_weak(
            empty_val, 
            index , 
            std::memory_order_release ,
            std::memory_order_relaxed))
            break;
    }
    }
    //oom
    if(unlikely(cnt == BUCKET_MAX))
        return false;

    // time_elasped<std::chrono::nanoseconds> tm{write_tm};

    //write
    memcpy_avx_16(entry[index].key , key.data());
    memcpy_avx_80(entry[index].value , value.data());

    bitset.set(hash % bitset.max_index);

    return true;
}

NvmEngine::~NvmEngine() {}
