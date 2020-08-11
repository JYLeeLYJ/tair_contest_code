#ifndef TAIR_CONTEST_KV_CONTEST_NVM_ENGINE_H_
#define TAIR_CONTEST_KV_CONTEST_NVM_ENGINE_H_

#include <libpmem.h>

#include <cstdio>
#include <cstring>
#include <include/db.hpp>
#include <mutex>
#include <string>
#include <unordered_map>

using std::string;

class LogAppender {
public:
    LogAppender(char* file_name, size_t size);
    std::pair<> (Slice& key, Slice& val);

    struct log_t {
        uint32_t key_len;
        uint32_t val_len;
    };

private:
    union {
        uint64_t* sequence;
        char* pmem_base;
    } pmem;
    size_t mapped_len;
    int is_pmem;
    uint64_t end_off;
};

class NvmExample : DB {
public:
    std::unordered_map<string, string> data;
    std::mutex mut;
    const int SIZE = 0x1000000;

    static Status CreateOrOpen(const std::string& name, DB** dbptr, FILE* log_file = nullptr);

    NvmExample(const std::string& name, FILE* log_file);
    Status Get(const Slice& key, std::string* value) override;
    Status Set(const Slice& key, const Slice& value) override;
    void Persist(void* addr, uint32_t len);
    void do_log(const struct Slice* key, const struct Slice* val);
    ~NvmExample() override;

    Status recover(const char* path, uint32_t size);
};

#endif