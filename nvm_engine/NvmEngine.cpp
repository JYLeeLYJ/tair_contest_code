#include <functional>
#include <limits>

#include <signal.h>

#include "NvmEngine.hpp"
#include "include/utils.hpp"
#include "include/logger.hpp"
#include "fmt/format.h"

//=============== Logger usage =================================

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
    
    static std::atomic<std::size_t> cnt{0} , not_found{0};
    auto local_cnt = cnt++;

    if(unlikely((local_cnt % 10000000) == 0)){
        Logger::instance().log(fmt::format("[Get]cnt = {} ,  {}% miss " ,local_cnt , not_found , (not_found * 100)/(local_cnt+1)));
    }

    auto k = key.to_string();
    auto * rec = find(k);
    if (rec)
        *value = rec->value();
    else 
        ++ not_found;
    return Ok;
}

Status NvmEngine::Set(const Slice& key, const Slice& value) {

    static std::atomic<std::size_t> cnt{0} , oom_cnt{0};
    auto local_cnt = cnt++;

    if(unlikely((local_cnt % 10000000) == 0)){
        Logger::instance().log(fmt::format("[Set]cnt = {} , {} % miss" ,local_cnt , (oom_cnt*100)/(local_cnt + 1)));
    }

    auto * rec = find(key.to_string());
    auto sta = Ok;
    if (rec){
        rec->update_value(value);
    }else 
        sta = append_new_value(key , value) ? Ok : OutOfMemory;
    if(sta == OutOfMemory) ++ oom_cnt;
    return sta;
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

    //linear search , from end to begin
    auto cnt = head.value_cnt.load();
    for (int i = cnt -1 ; i>=0  ; --i){
        auto & rec = pool.get_value(bk.indics[i]);
        if (rec.key() == key) {
            head.recent_vis_index = i;
            return &rec;
        }
    }

    //not found result
    return nullptr;
}

bool NvmEngine::append_new_value(const Slice & key , const Slice & value){
    auto k = key.to_string();
    uint32_t i_bk = std::hash<std::string>{}(k) % HASH_BUCKET_SIZE;

    auto index = pool.allocate_seq();
    if(!pool.is_valid_index(index)) {
        return false;
    }
    
    if(!hash_index.append(i_bk ,index)){
        return false;
    }
    
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
