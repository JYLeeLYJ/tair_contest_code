#include "NvmEngine.hpp"
#include "include/utils.hpp"
#include "include/logger.hpp"
#include "fmt/format.h"

#include <tuple>

#include <libpmem.h>
#include <sys/mman.h>
#include <unistd.h>
#include <signal.h>

#include <cassert>

void catch_bus_error(int sig){
    Logger::instance().sync_log("SIGBUS");
    exit(1);
}

void catch_segfault_error(int sig){
    Logger::instance().sync_log("SIG_FAULT.");
    Logger::instance().end_log();
    exit(1);
}

Status DB::CreateOrOpen(const std::string &name, DB **dbptr, FILE *log_file) {
    Logger::instance().set_file(log_file);
    return NvmEngine::CreateOrOpen(name, dbptr);
}

DB::~DB() {}

Status NvmEngine::CreateOrOpen(const std::string &name, DB **dbptr) {
    *dbptr = new NvmEngine(name);

    signal(SIGBUS  , catch_bus_error);
    signal(SIGSEGV , catch_segfault_error);
    return Ok;
}

NvmEngine::NvmEngine(const std::string &name) {

    bool is_exist = access(name.data() , 0) == 0;

    auto p = pmem_map_file(name.c_str(),NVM_SIZE,PMEM_FILE_CREATE, 0666, nullptr,nullptr);
    if(!p){
        perror("pmem map failed");
        exit(0);
    }

    file = decltype(file){p , NVM_SIZE};

    if(is_exist)
        recovery();

    Logger::instance().sync_log("*************init*****************");
}

Status NvmEngine::Get(const Slice &key, std::string *value) {

    auto kk = key.to_string();

    auto hash = hash_bytes_16(key.data());
    auto head = search(key , hash);

    Logger::instance().sync_log(fmt::format("[{}] get key = {}" , head ? "Found" : "NotFound" , kk));
    if(head){
        value->assign(reinterpret_cast<char *>(& file.value_blocks[head->beg]) , head->len);
        return Ok;
    }else{
        return NotFound;
    }
}

Status NvmEngine::Set(const Slice &key, const Slice &value) {

    static thread_local uint32_t bucket_id = get_bucket_id() ;

    auto hash = hash_bytes_16(key.data());
    auto head = search(key , hash);

    Logger::instance().sync_log(fmt::format("[Set] {} , {}" , bucket_id , key.to_string()));

    auto sta = Ok;
    if(head)
        sta = update(value , hash , head , bucket_id);
    else 
        sta = append(key,value , hash , bucket_id);

    return sta;
}

void NvmEngine::recovery(){

    std::array< std::thread  , THREAD_CNT> ts;

    for( auto & t : ts ) {
        t = std::thread{
            [this]{
                auto bk_id = get_bucket_id();
                //read head and build index
                uint32_t max_off = 0 , len = 0;
                for(;;){
                    auto key_index = new_key_info(bk_id);
                    auto & head = file.key_heads[key_index];

                    if(head.len == 0 && head.beg == 0){
                        -- bucket_infos[bk_id].key_seq;
                        break;
                    }

                    if(head.beg >= max_off){
                        max_off = head.beg ;
                        len = head.len;
                    }

                    auto index_id = new_index_info(bk_id);

                    using prefix_t = decltype(decltype(index)::index_info{}.prefix);
                    auto prefix = * reinterpret_cast<const prefix_t *>(head.key); 
                    auto hash = hash_bytes_16(head.key);
                    index.insert( hash % HASH_SIZE , index_id , key_index , prefix);   
                }
                //simple init allocator with some space waste
                uint32_t n_block = len == 0 ? 0 : (len / sizeof(value_block) + 1);
                allocator[bk_id].allocate(max_off + n_block);
                //optional : init from bitmap
            }
        };
    }

    for(auto & t : ts){
        t.join();
    }
}

head_info * NvmEngine::search(const Slice & key , uint64_t hash){
    using prefix_t = decltype(decltype(index)::index_info{}.prefix);
    const auto prefix = *reinterpret_cast<const prefix_t *>(key.data());

    for(auto & info : index.get_bucket(hash % index.bucket_cnt)){
        auto & head = file.key_heads[info.index];
        if(info.prefix == prefix && fast_key_cmp_eq(key.data() , head.key))
            return & head;
    }

    return nullptr;
}

Status NvmEngine::update(const Slice & value , uint64_t hash , head_info * head , uint32_t bucket_id){
    auto n_block = head->len / sizeof(value_block) + 1;
    auto n_block_new = value.size() / sizeof(value_block) + 1;

    uint32_t beg = allocator[bucket_id].allocate(n_block_new);
    if(beg == decltype(allocator)::value_type::null_index) 
        return OutOfMemory ;

    allocator[bucket_id].deallocate(n_block , head->beg);
    auto new_head = *head ; 
    new_head.beg = beg;
    new_head.len = value.size();
    
    //direct copy
    // #ifdef LOCAL_TEST
    memcpy(&(file.value_blocks[beg]) , value.data() , value.size());
    memcpy(head , &new_head , sizeof(head_info));
    // #else
    // pmem_memcpy_persist(&(file.value_blocks[beg]) , value.data() , value.size());
    // pmem_memcpy_persist(head , &new_head , sizeof(head_info));
    // #endif

    return Ok;
}

Status NvmEngine::append(const Slice & key , const Slice & value , uint64_t hash , uint32_t bucket_id){
    using prefix_t = decltype(decltype(index)::index_info{}.prefix);

    auto n_block = value.size() / sizeof(value_block) + 1;
    const auto prefix = *reinterpret_cast<const prefix_t * >(key.data());

    //allocated seq and space
    uint32_t value_beg = allocator[bucket_id].allocate(n_block);
    if(value_beg == decltype(allocator)::value_type::null_index)
        return OutOfMemory;
    
    uint32_t key_index = new_key_info(bucket_id);

    //write data
    auto & new_head = file.key_heads[key_index];
    void * dst = &(file.value_blocks[value_beg]);

    head_info head;

    head.beg = value_beg;
    head.len = value.size();
    memcpy_avx_16(head.key , key.data());

    // #ifdef LOCAL_TEST
    memcpy(dst , value.data() , value.size());
    memcpy(&new_head , &head , sizeof(head_info));
    // #else
    // pmem_memcpy_persist(dst , value.data() , value.size());
    // pmem_memcpy_persist(&new_head , &head , sizeof(head_info));
    // #endif

    index.insert(hash % HASH_SIZE , new_index_info(bucket_id) , key_index , prefix);

    return Ok;
}

NvmEngine::~NvmEngine() {
    pmem_unmap(file.base() , NVM_SIZE);
}
