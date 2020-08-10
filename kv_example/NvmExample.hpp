#ifndef TAIR_CONTEST_KV_CONTEST_NVM_ENGINE_H_
#define TAIR_CONTEST_KV_CONTEST_NVM_ENGINE_H_

#include <include/db.h>
#include <libpmem.h>

#include <cstdio>
#include <cstring>
#include <mutex>
#include <string>
#include <unordered_map>

using std::string;

class NvmExample : DB {
public:
    struct log_t {
        uint32_t key_len;
        uint32_t val_len;
    };
    union {
        uint32_t* sequence;
        char* pmemaddr;
    } pmem;
    size_t mapped_len;
    int is_pmem;
    uint32_t end_off;
    std::unordered_map<string, string> data;
    std::mutex mut;


    static Status Recover(const std::string& name, DB** dbptr);
    Status Get(const Slice& key, std::string* value) override;
    Status Set(const Slice& key, const Slice& value) override;
    void persist(void* addr, uint32_t len);
    void do_log(const struct Slice* key, const struct Slice* val);
    ~NvmExample() override;

    Status recover(const char* path, uint32_t size);
};

#endif