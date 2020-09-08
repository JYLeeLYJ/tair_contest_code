#ifndef TAIR_CONTEST_KV_CONTEST_NVM_ENGINE_H_
#define TAIR_CONTEST_KV_CONTEST_NVM_ENGINE_H_

#include <unordered_map>
#include <mutex>
#include <atomic>

#include "include/db.hpp"
#include "value_pool.hpp"

class NvmEngine : DB {
    #ifdef LOCAL_TEST
    static constexpr size_t VALUE_SCALE = 64 * 1024 ;   //64K
    #else
    static constexpr size_t VALUE_SCALE = 48 * 16 * 1024 * 1024; //(48 + 1 ) M * 16 threads
    #endif
    static constexpr size_t HASH_BUCKET_SIZE = 48 * 1024 * 1024;
public:
    static Status CreateOrOpen(const std::string& name, DB** dbptr);
    Status Get(const Slice& key, std::string* value) override;
    Status Set(const Slice& key, const Slice& value) override;

    explicit NvmEngine(const std::string & file_name);
    ~NvmEngine() override;

    std::size_t size() const noexcept {
        return hash_index.size();
    }

private:
    std::mutex mut;
    value_pool<VALUE_SCALE> pool;

    std::unordered_map<std::string , uint32_t> hash_index;
};

#endif