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

    _bucket = new std::atomic<uint32_t>[BUCKET_MAX]{};
    // memset(bucket, 0, sizeof(uint32_t) * BUCKET_MAX);

    void * base = pmem_map_file(name.c_str(),ENTRY_MAX * sizeof(entry_t),PMEM_FILE_CREATE, 0666, nullptr,nullptr);
    if (!base) {
        perror("Pmem map file failed");
        exit(1);
    }

    _base_ptr = base ;

    std::size_t sz = NVM_SIZE;
    constexpr auto key_size = ENTRY_MAX * KEY_SIZE , value_size = ENTRY_MAX * VALUE_SIZE    ;

    _key = reinterpret_cast<key_t *>(align(256 , key_size , base , sz));
    sz -= key_size;
    base = reinterpret_cast<char *>(base) + key_size;
    _value = reinterpret_cast<value_t *>(align(4096 , value_size , base , sz));
    sz -= value_size;

    if(!_key || !_value){
        perror("allocate align space failed.");
        exit(1);
    }

    Logger::instance().sync_log(name);
    Logger::instance().sync_log("*************************");
}

Status NvmEngine::Get(const Slice &key, std::string *value) {

    static bool flag{false};
    static std::atomic<uint64_t> not_found_cnt{0};
    static thread_local uint64_t cnt{0};

    if(unlikely(flag == false)){
        flag = true;
        Logger::instance().log(fmt::format("*********** Start Get ***********"));
    }

    if(unlikely(cnt ++ % 5000000 == 0)){
        Logger::instance().log(fmt::format("not found = {}" , not_found_cnt));
    }

    uint64_t hash = std::hash<std::string>{}(key.to_string());
    uint32_t index {0} , bucket_i {std::numeric_limits<uint32_t>::max()};

    std::tie(index , bucket_i) = search(key , hash);

    if(likely(index)){
        value->assign(_value[index].data() , 80);
        return Ok;
    }else{
        ++ not_found_cnt;
        return NotFound;
    }
}

static thread_local uint64_t total_set_tm{0},append_tm {0} , search_tm{0} , write_tm{0} , setindex_tm{0};

Status NvmEngine::Set(const Slice &key, const Slice &value) {

    // time_elasped<std::chrono::nanoseconds> set_tm{total_set_tm};
    static thread_local uint32_t cnt{0};
    if(unlikely(cnt ++ % 5000000 == 0))
        Logger::instance().log(
            fmt::format("set cnt = {} , append = {} ms , search_tm = {} ms" , cnt , append_tm / 1000000 , search_tm / 1000000)
            // fmt::format("set time = {} ,append_time = {} , search in set = {} , write time = {} , setindex_tm = {}" ,
            // total_set_tm / 1000000 ,append_tm / 1000000, search_tm/1000000 , write_tm / 1000000 , setindex_tm / 1000000)
            );

    uint64_t hash = std::hash<std::string>{}(key.to_string());
    uint32_t index {0} , bucket_id {std::numeric_limits<uint32_t>::max()};

    if(unlikely(_bitset.test(hash % _bitset.max_index))){
        time_elasped<std::chrono::nanoseconds> tm{search_tm};
        std::tie(index , bucket_id) = search(key , hash);
    }else 
        bucket_id = hash % BUCKET_MAX;

    //update
    if(unlikely(index)){
        memcpy_avx_80(_value[index].data() , value.data());
        return Ok;
    }
    //insert
    else if(likely(bucket_id != std::numeric_limits<uint32_t>::max())){
        time_elasped<std::chrono::nanoseconds> tm{append_tm}; 
        if(append(key , value , bucket_id , hash)){
            _bitset.set(hash % _bitset.max_index);
            return Ok;
        }else
            return OutOfMemory;
    }
    //oom
    else{
        return OutOfMemory;
    }
}

std::pair<uint32_t,uint32_t> NvmEngine::search(const Slice & key , uint64_t hash){
    for (uint32_t i = 0 , bucket_id = hash % BUCKET_MAX; i < BUCKET_MAX ; i++) {
        //not found
        if(_bucket[bucket_id] == 0)
            return {0 , bucket_id};
        
        uint32_t index = _bucket[bucket_id];

        //found
        if (fast_key_cmp_eq(_key[index].data(), key.data()))
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
        if(_bucket[i]) continue;

        uint32_t empty_val {0};
        if(_bucket[i].compare_exchange_weak(
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
    memcpy_avx_16(_key[index].data() , key.data());
    memcpy_avx_80(_value[index].data() , value.data());

    return true;
}

NvmEngine::~NvmEngine() {}
