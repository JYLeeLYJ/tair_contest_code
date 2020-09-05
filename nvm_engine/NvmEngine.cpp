#include<functional>

#include "NvmEngine.hpp"
#include "include/utils.hpp"
#include "include/logger.hpp"

Status DB::CreateOrOpen(const std::string& name, DB** dbptr, FILE* log_file) {
    // Logger::set_file(log_file);
    // Logger::instance().log("start...");

    fprintf(log_file ,"[Start]fprintf\n");
    return NvmEngine::CreateOrOpen(name, dbptr);
}

DB::~DB() {}

Status NvmEngine::CreateOrOpen(const std::string& name, DB** dbptr) {
    *dbptr = new NvmEngine(name);
    return Ok;
}

Status NvmEngine::Get(const Slice& key, std::string* value) {
    std::lock_guard<std::mutex> lk(mut);

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

    auto k = key.to_string();
    // Logger::instance().log("[SET]"+key.to_string() + " , " + value.to_string());
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
:pool(file_name) ,seq(0) ,hash_index(SIZE) {
    Logger::instance().log("init");
}

NvmEngine::~NvmEngine() {}
