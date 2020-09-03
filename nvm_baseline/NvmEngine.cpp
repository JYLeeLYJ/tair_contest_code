#include "NvmEngine.hpp"
#include "include/utils.hpp"

constexpr size_t SIZE = 64 * 1024 * 1024;   //64M

Status DB::CreateOrOpen(const std::string& name, DB** dbptr, FILE* log_file) {
    return NvmEngine::CreateOrOpen(name, dbptr);
}

DB::~DB() {}

Status NvmEngine::CreateOrOpen(const std::string& name, DB** dbptr) {
    *dbptr = new NvmEngine(name);
    return Ok;
}

Status NvmEngine::Get(const Slice& key, std::string* value) {
    std::lock_guard<std::mutex> guard(mut);
    auto k = key.to_string();
    auto p = hash_map.find(k);
    if (p == hash_map.end())
        return NotFound;
    else 
        *value = hash_map[k];
    return Ok;
}

Status NvmEngine::Set(const Slice& key, const Slice& value) {
    std::lock_guard<std::mutex> guard(mut);
    hash_map[key.to_string()] = value.to_string();
    return Ok;
}

NvmEngine::NvmEngine(const std::string & file_name) 
:hash_map(SIZE){
    UNUSED(file_name);
}

NvmEngine::~NvmEngine() {}
