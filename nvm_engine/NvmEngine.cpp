#include "NvmEngine.hpp"
#include "include/utils.hpp"
#include "include/logger.hpp"
#include "fmt/format.h"

#include <tuple>

#include <libpmem.h>
#include <sys/mman.h>

#include <cassert>

Status DB::CreateOrOpen(const std::string &name, DB **dbptr, FILE *log_file) {
    Logger::instance().set_file(log_file);
    return NvmEngine::CreateOrOpen(name, dbptr);
}

DB::~DB() {}

Status NvmEngine::CreateOrOpen(const std::string &name, DB **dbptr) {
    *dbptr = new NvmEngine(name);
    return Ok;
}

using namespace std::chrono;
void test_single(void * p){
    auto t1 = high_resolution_clock::now();
    pmem_memset_persist(p , 0 , 16ULL * 1024 * 1024 * 1024);
    auto t2 = high_resolution_clock::now();

    Logger::instance().sync_log(fmt::format("single = {} ms", duration_cast<milliseconds>(t2 - t1).count()));
}

void test_multi(void * p){
    constexpr size_t thread_cnt = 16;
    constexpr size_t len =  16ULL * 1024 * 1024 * 1024 / thread_cnt;
    std::thread ts[thread_cnt];
    auto t1 = high_resolution_clock::now();
    for(size_t i = 0 ; i< thread_cnt ; i ++){
        ts[i] = std::thread([=]{
            pmem_memset_persist((char *)(p) + i * len,  0 , len);
        });
    }
    for(size_t i = 0 ; i < thread_cnt ; ++i){
        ts[i].join();
    }
    auto t2 = high_resolution_clock::now();
    Logger::instance().sync_log(fmt::format("single = {} ms", duration_cast<milliseconds>(t2 - t1).count()));
}

NvmEngine::NvmEngine(const std::string &name) {

    auto p = pmem_map_file(name.c_str(),NVM_SIZE,PMEM_FILE_CREATE, 0666, nullptr,nullptr);
    if(!p){
        perror("pmem map failed");
        exit(0);
    }

    Logger::instance().sync_log("*************test*****************");
    test_single(p);
    test_multi(p);
    Logger::instance().sync_log("*************init*****************");
}

Status NvmEngine::Get(const Slice &key, std::string *value) {
    return Ok;
}

Status NvmEngine::Set(const Slice &key, const Slice &value) {
    return Ok;
}

NvmEngine::~NvmEngine() {}
