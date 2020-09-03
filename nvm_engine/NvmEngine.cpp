#include<functional>

#include "NvmEngine.hpp"
#include "include/utils.hpp"

Status DB::CreateOrOpen(const std::string& name, DB** dbptr, FILE* log_file) {
    return NvmEngine::CreateOrOpen(name, dbptr);
}

DB::~DB() {}

Status NvmEngine::CreateOrOpen(const std::string& name, DB** dbptr) {
    *dbptr = new NvmEngine(name);
    return Ok;
}

Status NvmEngine::Get(const Slice& key, std::string* value) {
    std::lock_guard<std::mutex> lk(mut);
    auto index = std::hash<std::string>{}(key.to_string()) % SIZE;
    if (bits.test(index) == false)
        return NotFound;
    else{
        *value = pool.get_value(index);
        return Ok;
    } 
}

Status NvmEngine::Set(const Slice& key, const Slice& value) {
    std::lock_guard<std::mutex> lk(mut);
    auto index = std::hash<std::string>{}(key.to_string()) % SIZE;
    pool.write_value(index , value.to_string());
    bits.set(index);
    return Ok;
}

NvmEngine::NvmEngine(const std::string & file_name) {
    UNUSED(file_name);
}

NvmEngine::~NvmEngine() {}
