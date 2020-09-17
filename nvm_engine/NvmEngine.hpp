#ifndef TAIR_CONTEST_KV_CONTEST_NVM_ENGINE_H_
#define TAIR_CONTEST_KV_CONTEST_NVM_ENGINE_H_

#include <unordered_map>
#include <mutex>
#include <atomic>

#include "include/db.hpp"
#include "include/index_hasher.hpp"
#include "include/record_pool.hpp"
#include "include/boolean_filter.hpp"

class NvmEngine : DB {
    #ifdef LOCAL_TEST
    static constexpr size_t VALUE_SCALE = 800 * 1024 ;   //64K
    #else
    static constexpr size_t VALUE_SCALE = 48 * 16 * 1024 * 1024; // 48M * 16 threads
    #endif

    static constexpr size_t HASH_BUCKET_SIZE = VALUE_SCALE / 8;
public:
    static Status CreateOrOpen(const std::string& name, DB** dbptr);
    Status Get(const Slice& key, std::string* value) override;
    Status Set(const Slice& key, const Slice& value) override;

    explicit NvmEngine(const std::string & file_name);
    ~NvmEngine() override;

private:

    Status Update(std::string key , const Slice & value) ;
    Status Append(std::string key , const Slice & value) ;

private:

    Record * find(const std::string & key ) ;

    bool append_new_value(const Slice & key , const Slice & value);

private:
    //use memory ~ 768M / 2 = 364M
    bitmap_filter<VALUE_SCALE * 4> bitset{};
    //use memory ~ 768M * 4 + 96M * 2 = 3GB
    Hash<HASH_BUCKET_SIZE , 8> hash_index{};
    //O(1) space
    record_pool<VALUE_SCALE+10> pool;

    std::atomic<std::size_t> seq{0};
};

#endif