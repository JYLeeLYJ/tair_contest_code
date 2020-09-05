#ifndef TAIR_CONTEST_KV_CONTEST_NVM_ENGINE_H_
#define TAIR_CONTEST_KV_CONTEST_NVM_ENGINE_H_

#include <unordered_map>
#include <mutex>
#include <atomic>

#include "include/db.hpp"
#include "value_pool.hpp"

class NvmEngine : DB {
    static constexpr size_t SIZE = 64 * 1024 * 1024;   //64M
public:
    static Status CreateOrOpen(const std::string& name, DB** dbptr);
    Status Get(const Slice& key, std::string* value) override;
    Status Set(const Slice& key, const Slice& value) override;

    explicit NvmEngine(const std::string & file_name);
    ~NvmEngine() override;
private:
    std::mutex mut;
    value_pool<SIZE> pool;

    uint32_t seq{0};
    std::unordered_map<std::string , uint32_t> hash_index;
};

#endif