#include<functional>

#include "NvmEngine.hpp"
#include "include/utils.hpp"
// #include "include/logger.hpp"


Status DB::CreateOrOpen(const std::string& name, DB** dbptr, FILE* log_file) {
    // Logger::set_file(log_file);
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
        *value = pool.value(index).to_string();
        return Ok;
    } 
}

Status NvmEngine::Set(const Slice& key, const Slice& value) {
    std::lock_guard<std::mutex> lk(mut);
    auto index = std::hash<std::string>{}(key.to_string()) % SIZE;

    if(!pool.set_value(index , value.to_string()))
        return OutOfMemory;
    
    bits.set(index);
    return Ok;
}

NvmEngine::NvmEngine(const std::string & file_name) 
:pool(file_name){

}

NvmEngine::~NvmEngine() {}
