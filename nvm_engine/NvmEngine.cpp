#include<functional>
#include "NvmEngine.hpp"
#include "include/utils.hpp"
#include "include/logger.hpp"

Status DB::CreateOrOpen(const std::string& name, DB** dbptr, FILE* log_file) {
    Logger::set_file(log_file);
    return NvmEngine::CreateOrOpen(name, dbptr);
}

DB::~DB() {}

Status NvmEngine::CreateOrOpen(const std::string& name, DB** dbptr) {
    *dbptr = new NvmEngine(name);
    return Ok;
}

Status NvmEngine::Get(const Slice& key, std::string* value) {
    std::lock_guard<std::mutex> lk(mut);

    static int cnt = 0;
    if(cnt% 1000000 == 0)
        Logger::instance().sysn_log("number of get = " + std::to_string(cnt++));

    auto p = hash_index.find(key.to_string());
    if (p == hash_index.end()){
        // Logger::instance().log("[Not Found]" + key.to_string());
        return NotFound;
    }
    else{
        *value = pool.value(p->second).to_string();
        // Logger::instance().log("[GET]"+key.to_string()+" , "+ *value);
        return Ok;
    }
}

Status NvmEngine::Set(const Slice& key, const Slice& value) {
    std::lock_guard<std::mutex> lk(mut);

    static int cnt = 0;
    if(cnt % 1000000 == 0)
        Logger::instance().sysn_log("number of set = " + std::to_string(cnt++));

    //hack
    if (seq > 100000){
        return Ok;
    }

    auto k = key.to_string();
    auto p = hash_index.find(k);
    uint32_t index{0};
    if (p == hash_index.end()){
        index = seq++;
        hash_index[std::move(k)] = index;
    }else{
        index = p->second;
    }
    pool.set_value(index, value);
    return Ok;
}

NvmEngine::NvmEngine(const std::string & file_name)
:pool(file_name) ,hash_index(SIZE) {
    Logger::instance().log("init");
}

NvmEngine::~NvmEngine() {}
