
#include <NvmEngine.hpp>

Status DB::Recover(const std::string& name, DB** dbptr) {
    return NvmEngine::Recover(name, dbptr);
}

DB::~DB() {}

Status NvmEngine::Recover(const std::string& name, DB** dbptr) {
    return Ok;
}
Status NvmEngine::Get(const Slice& key, std::string* value) {
    return Ok;
}
Status NvmEngine::Set(const Slice& key, std::string& value) {
    return Ok;
}

NvmEngine::~NvmEngine() {}

