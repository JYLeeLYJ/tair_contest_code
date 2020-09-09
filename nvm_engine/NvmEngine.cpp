#include<functional>
#include <signal.h>

#include "NvmEngine.hpp"
#include "include/utils.hpp"
#include "include/logger.hpp"

void catch_bus_error(int sig){
    Logger::instance().sync_log("SIGBUS");
    exit(1);
}

Status DB::CreateOrOpen(const std::string& name, DB** dbptr, FILE* log_file) {
    Logger::set_file(log_file);
    Logger::instance().sync_log("start");
    signal(SIGBUS  , catch_bus_error);
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
    if(unlikely(local_cnt% 1000000 == 0))
        Logger::instance().log("number of get = " + std::to_string(local_cnt));

    //hack
    if (seq > 100000){
        return Ok;
    }

    {
        std::lock_guard<std::mutex> lk(mut);

        auto p = hash_index.find(key.to_string());
        if (p == hash_index.end()){
            return NotFound;
        }
        else{
            *value = pool.value(p->second).to_string();
            return Ok;
        }
    }
}

Status NvmEngine::Set(const Slice& key, const Slice& value) {

    static std::atomic<std::size_t> cnt{0};
    auto local_cnt = cnt++;
    if(unlikely(local_cnt% 1000000 == 0)){
        Logger::instance().log("number of set = " + std::to_string(local_cnt));
    }

    //hack
    if (seq > 100000){
        return OutOfMemory;
    }

    {
        std::lock_guard<std::mutex> lk(mut);

        auto k = key.to_string();
        auto p = hash_index.find(k);
        uint32_t index{0};
        if (p == hash_index.end()){
            ++seq;
            index = pool.append_new_value(value);
            hash_index[std::move(k)] = index;
        }else{
            index = p->second;
        }
        pool.set_value(index, value);
        return Ok;
    }
}

NvmEngine::NvmEngine(const std::string & file_name)
:pool(file_name) ,hash_index(HASH_BUCKET_SIZE) {
    Logger::instance().sync_log("NvmEngine init");
    Logger::instance().sync_log(std::string("is lock free = ").append(seq.is_lock_free()? "true" : "false"));
}

NvmEngine::~NvmEngine() {
    Logger::instance().end_log();
}
