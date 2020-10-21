#include "NvmEngine.hpp"
#include "include/utils.hpp"
#include "include/logger.hpp"
#include "fmt/format.h"

#include <tuple>

#include <libpmem.h>
#include <sys/mman.h>
#include <unistd.h>

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

NvmEngine::NvmEngine(const std::string &name) {

    bool is_exist = access(name.data() , 0);

    auto p = pmem_map_file(name.c_str(),NVM_SIZE,PMEM_FILE_CREATE, 0666, nullptr,nullptr);
    if(!p){
        perror("pmem map failed");
        exit(0);
    }

    file = decltype(file){p , NVM_SIZE};

    //TODO:check recovery
    // if(is_exist)
    //     recovery();

    Logger::instance().sync_log("*************init*****************");
}

Status NvmEngine::Get(const Slice &key, std::string *value) {

    auto hash = hash_bytes_16(key.data());
    auto head = search(key , hash);

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

    if(head)
        return update(value , hash , head , bucket_id);
    else 
        return append(key,value , hash , bucket_id);

}

void NvmEngine::recovery(){
    //TODO: 
}

head_info * NvmEngine::search(const Slice & key , uint64_t hash){

    const auto prefix = *reinterpret_cast<const uint32_t *>(key.data());

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
    #ifdef LOCAL_TEST
    memcpy(&(file.value_blocks[beg]) , value.data() , value.size());
    memcpy(head , &new_head , sizeof(head_info));
    #else
    pmem_memcpy_persist(&(file.value_blocks[beg]) , value.data() , value.size());
    pmem_memcpy_persist(head , &new_head , sizeof(head_info));
    #endif

    return Ok;
}

Status NvmEngine::append(const Slice & key , const Slice & value , uint64_t hash , uint32_t bucket_id){
    auto n_block = value.size() / sizeof(value_block) + 1;
    const auto prefix = *reinterpret_cast<const uint32_t *>(key.data());

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

    #ifdef LOCAL_TEST
    memcpy(&new_head , &head , sizeof(head_info));
    memcpy(dst , value.data() , value.size());
    #else
    pmem_memcpy_persist(dst , value.data() , value.size());
    pmem_memcpy_persist(&new_head , &head , sizeof(head_info));
    #endif

    index.insert(hash % HASH_SIZE , new_index_info(bucket_id) , key_index , prefix);

    return Ok;
}

NvmEngine::~NvmEngine() {
    pmem_unmap(file.base() , NVM_SIZE);
}
