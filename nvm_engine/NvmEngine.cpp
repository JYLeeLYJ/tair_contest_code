#include <functional>
#include <limits>

#include <signal.h>

#include "NvmEngine.hpp"
#include "include/utils.hpp"
#include "include/logger.hpp"
#include "fmt/format.h"

//=============== Logger usage =================================
#ifdef LOCAL_TEST
constexpr uint64_t LOG_FEQ = 1e3;   //10 k
#else
constexpr uint64_t LOG_FEQ = 2e6;   //10 m
#endif

// std::atomic<uint64_t> find_cnt{0};

void catch_bus_error(int sig){
    Logger::instance().sync_log("SIGBUS");
    exit(1);
}

void catch_segfault_error(int sig){
    Logger::instance().sync_log("SIG_FAULT.");
    Logger::instance().end_log();
    exit(1);
}

inline const char * sta_to_string( const Status sta ){
    if(sta == NotFound) return "NotFound";
    else if(sta == OutOfMemory ) return "OutOfMemory";
    else if(sta == IOError) return "IOError";
    else return "Ok";
}

inline std::string to_int_string(const Slice & key){
    return fmt::format("{}_{}" ,* reinterpret_cast<const uint64_t *>(key.data()) , * reinterpret_cast<const uint64_t *>(key.data() + 8));
}

//================================================================

Status DB::CreateOrOpen(const std::string& name, DB** dbptr, FILE* log_file) {

    Logger::set_file(log_file);
    Logger::instance().sync_log("log start");

    signal(SIGBUS  , catch_bus_error);
    signal(SIGSEGV , catch_segfault_error);

    return NvmEngine::CreateOrOpen(name, dbptr);
}

DB::~DB() {}

Status NvmEngine::CreateOrOpen(const std::string& name, DB** dbptr) {
    *dbptr = new NvmEngine(name);
    return Ok;
}

Status NvmEngine::Get(const Slice& key, std::string* value) {

    static bool flag{false};
    if(!flag){
        Logger::instance().log("******** Start Get *********** .");
        flag = true;
    }

    std::string key_str = key.to_string();
    uint64_t hash_value = std::hash<std::string>{}(key_str);
    
    auto * rec = find(key_str , hash_value);
    if (rec)
        *value = rec->value();
    return Ok;
}

static std::atomic<uint64_t> tm_append{0};

Status NvmEngine::Set(const Slice& key, const Slice& value) {

    static thread_local uint64_t cnt {0};
    static std::atomic<uint64_t> miss_cnt{0} , oom{0};
    static std::atomic<uint64_t> tm_find{0};
    if(unlikely((cnt ++ % LOG_FEQ) == 0)){
        Logger::instance().log(fmt::format("total find time = {} us , find_cnt = {} , oom = {}" , tm_find , miss_cnt , oom));
    }

    std::string key_str = key.to_string();
    uint64_t hash_value = std::hash<std::string>{}(key_str);

    Status sta = Ok;

    //confict : found in bitset then emit find()
    if(unlikely(bitset.test(hash_value % bitset.max_index))){
        // ++filter_miss_cnt;
        ++miss_cnt;
        Record * rec = nullptr;

        {
        auto beg = std::chrono::high_resolution_clock::now();
        rec = find(key_str , hash_value);
        auto end = std::chrono::high_resolution_clock::now();
        
        tm_find += std::chrono::duration_cast<std::chrono::microseconds>(end- beg).count();
        }
        
        if(rec) 
            rec->update_value(value);  
        else {
            sta = append_new_value(key , value , hash_value) ? Ok : OutOfMemory;
        }
    }else{
        sta = append_new_value(key , value , hash_value) ? Ok : OutOfMemory;
    }

    if(unlikely(sta == OutOfMemory)) ++oom;
    return sta;
}

Record * NvmEngine::find(const std::string & key , uint64_t hash_value) {
        
    uint32_t i_bk =  hash_value % HASH_BUCKET_SIZE;
    auto bk_pair = hash_index.get_bucket(i_bk) ;
    auto & head = bk_pair.first;
    auto & bk = bk_pair.second;

    //check recent vis;
    uint8_t recent_i = head.recent_vis_index;
    if(likely(recent_i != std::numeric_limits<uint8_t>::max())){
        Record & recent_rec = pool.get_value(bk.indics[recent_i]);
        if(fast_key_cmp_eq(recent_rec.key_data() , key.data())){
            return &recent_rec;
        }
    }

    //linear search , from end to begin
    auto cnt = head.value_cnt.load();
    for (int i = cnt -1 ; i>=0  ; --i){
        auto & rec = pool.get_value(bk.indics[i]);
        if(fast_key_cmp_eq(rec.key_data() , key.data())){
            head.recent_vis_index = i;
            return &rec;
        }
    }

    //not found result
    return nullptr;
}

bool NvmEngine::append_new_value(const Slice & key , const Slice & value , uint64_t hash_value){
    uint32_t i_bk = hash_value % HASH_BUCKET_SIZE;

    auto index = pool.allocate_seq(hash_value);
    if(unlikely(!pool.is_valid_index(index))) {
        return false;
    }
    
    if(unlikely(!hash_index.append(i_bk ,index))){
        return false;
    }
    
    pool.set_value(index , key , value);
    bitset.set(hash_value % bitset.max_index);
    return true;
}

NvmEngine::NvmEngine(const std::string & file_name)
:pool(file_name) {
    Logger::instance().sync_log(fmt::format("hash bucket len = {}" , hash_index.bk_len));
    Logger::instance().sync_log("NvmEngine init\n*******************************");
}

NvmEngine::~NvmEngine() {
    Logger::instance().end_log();
}
