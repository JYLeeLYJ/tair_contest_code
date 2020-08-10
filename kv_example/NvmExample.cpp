#include <libpmem.h>

#include <NvmExample.hpp>
#include <cstdio>
#include <cstring>
#include <mutex>
#include <string>
#include <unordered_map>

Status DB::Recover(const std::string& name, DB** dbptr, FILE* log_file = nullptr) {
    return NvmExample::Recover(name, dbptr);
}

DB::~DB() {}

void NvmExample::persist(void* addr, uint32_t len) {
    if (is_pmem)
        pmem_persist(addr, len);
    else
        pmem_msync(addr, len);
}

void NvmExample::do_log(const struct Slice* key, const struct Slice* val) {
    log_t log;
    log.key_len = key->size;
    if (val) {
        log.val_len = val->size;
    }
    char* ptr = pmem.pmemaddr + end_off;
    memcpy(ptr, &log, sizeof(log_t));
    persist(ptr, sizeof(log));
    ptr += sizeof(log_t);
    memcpy(ptr, key->data, log.key_len);
    persist(ptr, log.key_len);
    ptr += log.key_len;
    memcpy(ptr, val->data, log.val_len);
    persist(ptr, log.val_len);
    ptr += log.val_len;
    end_off = ptr - pmem.pmemaddr;
    (*pmem.sequence)++;
    persist(pmem.sequence, sizeof(uint32_t));
}

Status NvmExample::recover(const char* path, uint32_t size) {
    if ((pmem.pmemaddr = (char*)pmem_map_file(path, size, PMEM_FILE_CREATE, 0666, &mapped_len, &is_pmem)) == NULL) {
        return IOError;
    }
    if (*pmem.sequence != 0) {
        char* now = pmem.pmemaddr + 4;
        for (unsigned int i = 0; i < *pmem.sequence; i++) {
            log_t* log = (log_t*)now;
            now += sizeof(log_t);
            string key(now, log->key_len);
            now += log->key_len;
            string val(now, log->val_len);
            now += log->val_len;
            data[std::move(key)] = std::move(val);
        }
        end_off = now - pmem.pmemaddr;
    } else {
        end_off = 4;
    }
    return Ok;
}

Status NvmExample::Recover(const std::string& name, DB** dbptr, FILE* log_file = nullptr) {
    NvmExample* db = new NvmExample;
    *dbptr = db;
    const int SIZE = 0x1000000;
    return db->recover(name.c_str(), SIZE);
}

Status NvmExample::Get(const Slice& key, std::string* value) {
    std::string k(key.data, key.size);
    std::lock_guard<std::mutex> lock(mut);
    auto kv = data.find(std::move(k));
    if (kv == data.end()) {
        return NotFound;
    }
    value->assign(kv->second);
    return Ok;
}

Status NvmExample::Set(const Slice& key, const Slice& value) {
    std::string key_s(key.data, key.size);
    std::string val_s(value.data, value.size);
    std::lock_guard<std::mutex> lock(mut);
    do_log(&key, &value);
    data[std::move(key_s)] = std::move(val_s);
    return Ok;
}

NvmExample::~NvmExample() {}
