#ifndef TAIR_CONTEST_KV_CONTEST_NVM_ENGINE_H_
#define TAIR_CONTEST_KV_CONTEST_NVM_ENGINE_H_

#include <atomic>
#include <climits>
#include <cstddef>
#include <cstdlib>
#include <iostream>
#include <atomic>

#include "include/db.hpp"

class NvmEngine : DB {
public:
    /**
     * @param 
     * name: file in AEP(exist)
     * dbptr: pointer of db object
     *
     */
    static Status CreateOrOpen(const std::string &name, DB **dbptr);
    NvmEngine(const std::string &name);
    Status Get(const Slice &key, std::string *value);
    Status Set(const Slice &key, const Slice &value);
    ~NvmEngine();

private:
    #ifdef LOCAL_TEST
    static constexpr size_t NVM_SIZE = 64ULL * 1024 ;
    #else
    static constexpr size_t NVM_SIZE = 64ULL * 1024 * 1024 * 1024 ;
    #endif

private:
    void recovery();
};

#endif
