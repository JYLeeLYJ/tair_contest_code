#ifndef TAIR_CONTEST_KV_CONTEST_NVM_ENGINE_H_
#define TAIR_CONTEST_KV_CONTEST_NVM_ENGINE_H_

#include "include/db.hpp"
#include <mutex>
#include <unordered_map>

class NvmEngine : DB {
public:
    static Status CreateOrOpen(const std::string& name, DB** dbptr);
    Status Get(const Slice& key, std::string* value);
    Status Set(const Slice& key, const Slice& value);

    explicit NvmEngine(const std::string & file_name);
    ~NvmEngine();
private:
    std::mutex mut;
    std::unordered_map<std::string , std::string> hash_map;
};

#endif