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

#ifdef USE_LIBPMEM
    if ((entry = (entry_t *)(pmem_map_file(name.c_str(),
                                          ENTRY_MAX * sizeof(entry_t),
                                          PMEM_FILE_CREATE, 0666, nullptr,
                                          nullptr))) == NULL) {
        perror("Pmem map file failed");
        exit(1);
    }
#else
    if ((entry = (entry_t *)(mmap(NULL, ENTRY_MAX * sizeof(entry_t),
                                 PROT_READ | PROT_WRITE,
                                 MAP_ANON | MAP_SHARED, 0, 0))) == NULL) {
        perror("mmap failed");
        exit(1);
    }
#endif
    Logger::instance().sync_log(name);
    Logger::instance().sync_log("*************************");
}

Status NvmEngine::Get(const Slice &key, std::string *value) {

    static bool flag{false};
    if(unlikely(flag == false)){
        flag = true;
        Logger::instance().log(fmt::format("*********** Start Get ***********"));
    }

    uint64_t hash = std::hash<std::string>{}(key.to_string());
    uint32_t index {0} , bucket_i {std::numeric_limits<uint32_t>::max()};

    std::tie(index , bucket_i) = search(key , hash);

    if(likely(index)){
        value->assign(entry[index].value , 80);
        return Ok;
    }else{
        return NotFound;
    }
}

static thread_local uint64_t append_tm {0} , search_tm{0};

Status NvmEngine::Set(const Slice &key, const Slice &value) {

    static thread_local uint32_t cnt{0};
    if(unlikely(cnt ++ % 5000000 == 0))
        Logger::instance().log(fmt::format("append_time = {} , search in set = {}" , append_tm / 1000000, search_tm/1000000));

    uint64_t hash = std::hash<std::string>{}(key.to_string());
    uint32_t index {0} , bucket_id {std::numeric_limits<uint32_t>::max()};

    if(unlikely(bitset.test(hash % bitset.max_index))){
        time_elasped<std::chrono::nanoseconds> tm{search_tm};
        std::tie(index , bucket_id) = search(key , hash);
    }else 
        bucket_id = hash % BUCKET_MAX;

    //update
    if(unlikely(index)){
        memcpy(entry[index].value , value.data() , 80);
        return Ok;
    }
    //insert
    else if(likely(bucket_id != std::numeric_limits<uint32_t>::max())){
        time_elasped<std::chrono::nanoseconds> tm{append_tm}; 
        if(append(key , value , bucket_id)){
            bitset.set(hash % bitset.max_index);
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

bool NvmEngine::append(const Slice & key , const Slice & value, uint32_t i){
    //allocated index 
    uint32_t index = entry_cnt ++ ;
    //oom , full entry pool
    if(unlikely(index >= ENTRY_MAX))
        return false;

    for (uint32_t cnt = 0 ; cnt < BUCKET_MAX ; ++cnt ){
        auto & atm_ui = bucket[i];

        uint32_t empty_val {0};
        if(atm_ui.compare_exchange_weak(empty_val, index , std::memory_order_release ,std::memory_order_relaxed)){
            //write entry
            memcpy(entry[index].key , key.data() , 16);
            memcpy(entry[index].value , value.data() , 80);
            return true;
        }
        
        ++i;
        i %= BUCKET_MAX;
    }
    //oom , full bucket
    return false;
}

NvmEngine::~NvmEngine() {}
