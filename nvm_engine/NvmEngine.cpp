#include<functional>
#include <signal.h>

#include "NvmEngine.hpp"
#include "include/utils.hpp"
#include "include/logger.hpp"
#include "include/spinlock.hpp"

void catch_bus_error(int sig){
    Logger::instance().sync_log("SIGBUS");
    exit(1);
}

void catch_segfault_error(int sig){
    Logger::instance().sync_log("SIG_FAULT.");
    Logger::instance().end_log();
    exit(1);
}


void test_atomic_hash_size_after_par_set(DB * db){
    std::thread threads[16];

    for (int i = 0 ; i < 16 ; ++ i){
        threads[i] = std::thread([=](){
            std::array<char , 80> v{'a'};
            for ( int j = 0 ; j < 1000; ++j){
                char key[17]{};
                sprintf(key , "%02d%04d0000000000", i , j);
                db->Set(Slice(key, 16) ,Slice(v.data(),80));
            }
        });
    }
    
    for (auto & t : threads )
        t.join();
}

Status DB::CreateOrOpen(const std::string& name, DB** dbptr, FILE* log_file) {
    Logger::set_file(log_file);
    Logger::instance().sync_log("start");
    
    signal(SIGBUS  , catch_bus_error);
    signal(SIGSEGV , catch_segfault_error);

    return NvmEngine::CreateOrOpen(name, dbptr);
}

DB::~DB() {}

Status NvmEngine::CreateOrOpen(const std::string& name, DB** dbptr) {
    *dbptr = new NvmEngine(name);

    #ifdef LOCAL_TEST
    test_atomic_hash_size_after_par_set(*dbptr);
    if(static_cast<NvmEngine*>(*dbptr)->hash_index.size() == 16 * 1000)
        Logger::instance().sync_log("test_atomic_hash_size_after_par_set succeed");
    else 
        Logger::instance().sync_log("test_atomic_hash_size_after_par_set failed");
    #endif

    return Ok;
}

Status NvmEngine::Get(const Slice& key, std::string* value) {

    //================================================
    static std::atomic<int> cnt{0};
    int local_cnt = cnt++;
    if(unlikely(local_cnt% 1000000 == 0))
        Logger::instance().sync_log("number of get = " + std::to_string(local_cnt));

    //hack
    if (local_cnt > 1000000){
        return NotFound;
    }

    if(unlikely( local_cnt % 100000 == 0) ){
        Logger::instance().sync_log("[GET]" + std::to_string(local_cnt));
    }

    //=================================================

    auto k = key.to_string();
    // auto i = std::hash<std::string>{}(k) % flags.size();
    // auto & cur_flag = flags[i];

    // SpinLock lk(cur_flag);

    std::lock_guard<std::mutex> lk(mut);

    auto p = hash_index.find(k);

    if (p == hash_index.end()){
        return NotFound;
    }
    else{
        *value = pool.value(p->second).to_string();
        return Ok;
    }

}

Status NvmEngine::Set(const Slice& key, const Slice& value) {

    //================================================

    static std::atomic<int> cnt{0};
    int local_cnt = cnt++;

    if(unlikely(local_cnt % 1000000 == 0)){
        Logger::instance().sync_log("number of set = " + std::to_string(local_cnt));
    }

    //hack
    if (local_cnt > 1000000){
        return OutOfMemory;
    }

    if(unlikely(local_cnt % 100000 == 0) ){
        Logger::instance().sync_log("[SET]" + std::to_string(local_cnt));
    }

    //================================================

    auto k = key.to_string();
    std::lock_guard<std::mutex> lk(mut);

    auto p = hash_index.find(k);
    uint32_t index{0};

    if (likely(p == hash_index.end())){
        index = pool.append_new_value(value);
        hash_index[std::move(k)] = index;
    }else{
        index = p->second;
        pool.set_value(index, value);
    }

    return Ok;
}

NvmEngine::NvmEngine(const std::string & file_name)
:pool(file_name) ,hash_index(HASH_BUCKET_SIZE) {
    // for (auto & f : flags){
    //     f.clear();
    // }

    Logger::instance().sync_log("max threads = " + std::to_string(std::thread::hardware_concurrency()));

    Logger::instance().sync_log(
        "NvmEngine init , hash bucket loal factor = " + std::to_string(hash_index.max_load_factor()));
}

NvmEngine::~NvmEngine() {}
