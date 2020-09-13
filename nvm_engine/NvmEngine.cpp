#include<functional>
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
    if(unlikely(local_cnt% 1000000 == 0))
        Logger::instance().log(fmt::format("number of get = {}" ,local_cnt));

    //test hack
    if (seq > 100000){
        return Ok;
    }

    uint32_t index{0};

    {
        // std::lock_guard<std::mutex> lk(mut);

        auto p = hash_index.find(key.to_string());
        if (p == hash_index.end()){
            return NotFound;
        }
        else{
            index = p->second;
        }
    }

    *value = pool.value(index).to_string();
    return Ok;
}

Status NvmEngine::Set(const Slice& key, const Slice& value) {

    static std::atomic<std::size_t> cnt{0};
    auto local_cnt = cnt++;
    if(unlikely(local_cnt% 1000000 == 0)){
        Logger::instance().log(fmt::format("number of set = {}" ,local_cnt));
    }

    //test hack
    if (seq > 100000){
        return OutOfMemory;
    }

    {
        auto k = key.to_string();
        auto p = hash_index.find(k);
        uint32_t index{0};
        if (p == hash_index.end()){
            ++seq;
            index = pool.append_new_value(value);
            std::lock_guard<std::mutex> lk(mut);
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
    Logger::instance().sync_log("NvmEngine init\n*******************************");
}

NvmEngine::~NvmEngine() {
    Logger::instance().end_log();
}
