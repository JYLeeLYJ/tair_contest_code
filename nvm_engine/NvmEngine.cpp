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
    Logger::instance().sync_log(name);
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

    mmap_base = pmem_map_file(name.c_str(), NVM_SIZE ,PMEM_FILE_CREATE, 0666, nullptr,nullptr);

    if ( mmap_base == nullptr) {
        perror("Pmem map file failed");
        exit(1);
    }

    void * base = mmap_base;
    std::size_t sz = NVM_SIZE;
    constexpr std::size_t key_size = ENTRY_MAX * KEY_SIZE , value_size = ENTRY_MAX * VALUE_SIZE    ;

    key_array = reinterpret_cast<key_t *>(align(16 , key_size , base , sz));
    sz -= key_size;
    base = reinterpret_cast<char *>(base) + key_size;
    value_array = reinterpret_cast<value_t *>(align(4096 , value_size , base , sz));
    sz -= value_size;

    if(key_array == nullptr || value_array == nullptr){
        perror("Align space allocated failed.");
        exit(1);
    }

    Logger::instance().sync_log("*************************");
}

Status NvmEngine::Get(const Slice &key, std::string *value) {

    static bool flag{false};
    if(unlikely(flag == false)){
        flag = true;
        Logger::instance().log(fmt::format("*********** Start Get ***********"));
    }

    static thread_local uint32_t cnt{0};
    static std::atomic<uint64_t> not_found{0};
    if(unlikely(cnt ++ % 5000000 == 0))
        Logger::instance().log(
            fmt::format("cnt = {} , not found = {}" , cnt , not_found));


    uint64_t hash = std::hash<std::string>{}(key.to_string());
    uint32_t index {0} , bucket_i {std::numeric_limits<uint32_t>::max()};

    std::tie(index , bucket_i) = search(key , hash);

    if(likely(index)){
        value->assign(value_array[index].data() , VALUE_SIZE );
        return Ok;
    }else{
        ++not_found;
        return NotFound;
    }
}

static thread_local uint64_t total_set_tm{0},append_tm {0} , search_tm{0} , write_tm{0} , setindex_tm{0};

Status NvmEngine::Set(const Slice &key, const Slice &value) {

    // time_elasped<std::chrono::nanoseconds> set_tm{total_set_tm};
    static thread_local uint32_t cnt{0} , oom{0};
    if(unlikely(cnt ++ % 5000000 == 0))
        Logger::instance().log(
            fmt::format("set time = {} ,append_time = {} , search in set = {} , write time = {} , setindex_tm = {} , oom = {}" ,
            total_set_tm / 1000000 ,append_tm / 1000000, search_tm/1000000 , write_tm / 1000000 , setindex_tm / 1000000 , oom));

    uint64_t hash = std::hash<std::string>{}(key.to_string());
    uint32_t index {0} , bucket_id {std::numeric_limits<uint32_t>::max()};

    if(unlikely(bitset.test(hash % bitset.max_index))){
        time_elasped<std::chrono::nanoseconds> tm{search_tm};
        std::tie(index , bucket_id) = search(key , hash);
    }else 
        bucket_id = hash % BUCKET_MAX;

    //update
    if(unlikely(index)){
        memcpy_avx_80(value_array[index].data() , value.data());
        return Ok;
    }
    //insert
    else if(likely(bucket_id != std::numeric_limits<uint32_t>::max())){
        time_elasped<std::chrono::nanoseconds> tm{append_tm}; 
        if(append(key , value , bucket_id , hash)){
            bitset.set(hash % bitset.max_index);
            return Ok;
        }else{
            ++ oom;
            return OutOfMemory;
        }
    }
    //oom
    else{
        return OutOfMemory;
    }
}

std::pair<uint32_t,uint32_t> NvmEngine::search(const Slice & key , uint64_t hash){
    for (uint32_t i = 0 , bucket_id = hash % BUCKET_MAX; i < BUCKET_MAX ; i++) {
        //not found
        if(bucket[bucket_id] == 0)
            return {0 , bucket_id};
        
        uint32_t index = bucket[bucket_id];

        //found
        if (fast_key_cmp_eq(key_array[index].data(), key.data()))
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
    if(unlikely(!is_valid_index(index)))
        return false;

    //write index
    uint32_t cnt = 0 ;
    // {
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
    // }
    //oom
    if(unlikely(cnt == BUCKET_MAX))
        return false;

    // time_elasped<std::chrono::nanoseconds> tm{write_tm};

    //write
    memcpy_avx_16(key_array[index].data() , key.data());
    memcpy_avx_80(value_array[index].data() , value.data());

    return true;
}

NvmEngine::~NvmEngine() {}
