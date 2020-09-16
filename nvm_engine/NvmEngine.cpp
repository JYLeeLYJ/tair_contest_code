#include <functional>
#include <limits>

#include <signal.h>

#include "NvmEngine.hpp"
#include "include/utils.hpp"
#include "include/logger.hpp"
#include "fmt/format.h"

void catch_bus_error(int sig){
    Logger::instance().sync_log("SIGBUS");
    exit(1);
}

void catch_segfault_error(int sig){
    Logger::instance().sync_log("SIG_FAULT.");
    Logger::instance().end_log();
    exit(1);
}

bool is_get_set_test = false;

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
    
    static std::atomic<std::size_t> cnt{0};
    auto local_cnt = cnt++;
    // if(unlikely(local_cnt% 1000000 == 0))
        Logger::instance().log(fmt::format("number of get = {}" ,local_cnt));

    // static std::once_flag flag;
    // std::call_once(flag , []{is_get_set_test = true;});

    auto k = key.to_string();
    auto * rec = find(k);
    if(!rec)
        return NotFound;
    else {
        *value = rec->value();
    }    
    return Ok;
}

Status NvmEngine::Set(const Slice& key, const Slice& value) {

    static std::atomic<std::size_t> cnt{0};
    auto local_cnt = cnt++;
    // if(unlikely(local_cnt% 1000000 == 0)){
        Logger::instance().log(fmt::format("number of set = {}" ,local_cnt));
    // }

    // if(is_get_set_test){
    //     Logger::instance().sync_log("is_get_set_test = true");
    //     //search and update
    //     auto * rec = find(key.to_string());
    //     if(!rec) 
    //         rec->update_value(value);
    //     return Ok;
    // }else{
    //     Logger::instance().sync_log("is_get_set_test = false");
    //     //append only
    //     return append_new_value(key,value) ? Ok : OutOfMemory;
    // }

    auto * rec = find(key.to_string());
    if (rec){
        rec->update_value(value);
        return Ok;
    }
    else 
        return append_new_value(key , value) ? Ok : OutOfMemory;
}

Record * NvmEngine::find(const std::string & key) {
    uint32_t i_bk = std::hash<std::string>{}(key) % HASH_BUCKET_SIZE;
    auto bk_pair = hash_index.get_bucket(i_bk) ;
    auto & head = bk_pair.first;
    auto & bk = bk_pair.second;

    //check recent vis;
    uint8_t recent_i = head.recent_vis_index;
    if(likely(recent_i != std::numeric_limits<uint8_t>::max())){
        Record & recent_rec = pool.get_value(bk.indics[recent_i]);
        if( recent_rec.key() == key){
            return &recent_rec;
        }
    }

    // Logger::instance().sync_log(fmt::format("find key = {} , recent_vis_index = {} , recent_i = {}" , key , head.recent_vis_index , recent_i));
    //linear search , from end to begin
    auto cnt = head.value_cnt.load();
    // Logger::instance().sync_log(fmt::format("cnt = {}" , cnt));
    for (int i = cnt -1 ; i>=0  ; --i){
        auto & rec = pool.get_value(bk.indics[i]);
        if (rec.key() == key) {
            head.recent_vis_index = i;
            return &rec;
        }
    }
    //not found result
    // return std::numeric_limits<uint32_t>::max();
    return nullptr;
}

bool NvmEngine::append_new_value(const Slice & key , const Slice & value){
    auto k = key.to_string();
    uint32_t i_bk = std::hash<std::string>{}(k) % HASH_BUCKET_SIZE;

    auto index = pool.allocate_seq();
    if(!pool.is_valid_index(index)) 
        return false;
    
    if(!hash_index.append(i_bk ,index))
        return false;
    
    pool.set_value(index , key , value);
    return true;
}

NvmEngine::NvmEngine(const std::string & file_name)
:pool(file_name) {
    Logger::instance().sync_log("NvmEngine init\n*******************************");
}

NvmEngine::~NvmEngine() {
    Logger::instance().end_log();
}
