#include "NvmEngine.hpp"
#include "include/utils.hpp"
#include "include/logger.hpp"
#include "fmt/format.h"

#include <tuple>
#include <algorithm>
#include <numeric>

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
    Logger::instance().sync_log("SIGFAULT.");
    Logger::instance().end_log();
    exit(1);
}

constexpr auto LOG_SEQ = 3000000;

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

    Logger::instance().sync_log(fmt::format("hardware concurrency = {}" , std::thread::hardware_concurrency()));
    Logger::instance().sync_log(fmt::format("NVM_SIZE {} , N_KEY = {}" , NVM_SIZE ,N_KEY ));

    bool is_exist = access(name.data() , 0) == 0;

    auto p = pmem_map_file(name.c_str(),NVM_SIZE,PMEM_FILE_CREATE, 0666, nullptr,nullptr);
    if(!p){
        perror("pmem map failed");
        exit(0);
    }

    file = decltype(file){p , NVM_SIZE};

    for(uint32_t i = 0 ; i < BUCKET_CNT ; ++i ){
        allocator[i].reinit(i * decltype(allocator)::value_type::num_block , 0);
    }

    if(is_exist)
        recovery();

    Logger::instance().sync_log("************* Start ***************");
}

Status NvmEngine::Get(const Slice &key, std::string *value) {

    static thread_local uint64_t cnt {0};
    if(unlikely(cnt ++ % LOG_SEQ == 0) ){
        Logger::instance().log(fmt::format("[Get] cnt = {} " , cnt - 1));
    }

    auto hash = hash_bytes_16(key.data());
    auto info = search(key , hash);

    if(info){
        auto head = &file.key_heads[info->index];
        value->assign(reinterpret_cast<char *>(& file.value_blocks[head->beg]) , head->len);
        return Ok;
    }else{
        Logger::instance().sync_log("not found.");
        return NotFound;
    }
}

Status NvmEngine::Set(const Slice &key, const Slice &value) {

    static thread_local uint32_t bucket_id = get_bucket_id() ;

    static thread_local uint64_t cnt{0} , ud_cnt{0} , ap_cnt{0};
    if(unlikely(cnt ++ % LOG_SEQ == 0) ){
        Logger::instance().log(fmt::format("[Set] update = {} , append = {} ,blocks remains = {}" , ud_cnt , ap_cnt , allocator[bucket_id].print_space_use()));
    }

    auto hash = hash_bytes_16(key.data());
    auto info = search(key , hash);

    Status sta{Ok};
    if(info){
        ++ud_cnt;
        auto head = &file.key_heads[info->index];
        sta = update(value , hash , head , bucket_id);
    }
    else {
        sta = append(key,value , hash , bucket_id);
        ++ap_cnt;
    }

    if(unlikely(sta == OutOfMemory)){
        static thread_local std::once_flag flag;
        std::call_once(flag , [&]{
            Logger::instance().log(fmt::format("OOM , thread {} , new keys {} , blocks remains = {}" , bucket_id , bucket_infos[bucket_id].key_seq, allocator[bucket_id].print_space_use()));
        });
    }
    return sta;
}

NvmEngine::hash_t::index_info * NvmEngine::search(const Slice & key , uint64_t hash){
    using prefix_t = decltype(decltype(index)::index_info{}.prefix);
    const auto prefix = *reinterpret_cast<const prefix_t *>(key.data());

    for(auto & info : index.get_bucket(hash % index.bucket_cnt)){
        auto & head = file.key_heads[info.index];
        if(info.prefix == prefix && fast_key_cmp_eq(key.data() , head.key)){
            return & info;
        }
    }

    return nullptr;
}

Status NvmEngine::update(const Slice & value , uint64_t hash , head_info * head , uint32_t bucket_id){
    auto n_block = head->len / sizeof(value_block) + 1;
    auto n_block_new = value.size() / sizeof(value_block) + 1;

    uint32_t beg = alloc_value_blocks(bucket_id , n_block_new);
    if(beg == decltype(allocator)::value_type::null_index) 
        return OutOfMemory ;

    if(head->beg / sizeof(value_block) != bucket_id){
        static std::once_flag flag;
        std::call_once(flag , []{Logger::instance().sync_log("Out of Bucket!");});
    }
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
    using prefix_t = decltype(decltype(index)::index_info{}.prefix);

    auto n_block = value.size() / sizeof(value_block) + 1;
    const auto prefix = *reinterpret_cast<const prefix_t * >(key.data());

    //allocated seq and space
    uint32_t value_beg = alloc_value_blocks(bucket_id , n_block);
    if(value_beg == decltype(allocator)::value_type::null_index)
        return OutOfMemory;
    
    uint32_t key_index = new_key_info(bucket_id);
    if(unlikely(key_index == hash_t::null_id)){
        Logger::instance().sync_log("key pool is full!");
        return OutOfMemory;
    }

    //write data
    auto & new_head = file.key_heads[key_index];
    void * dst = &(file.value_blocks[value_beg]);

    head_info head;

    head.beg = value_beg;
    head.len = value.size();
    memcpy_avx_16(head.key , key.data());

    #ifdef LOCAL_TEST
    memcpy(dst , value.data() , value.size());
    memcpy(&new_head , &head , sizeof(head_info));
    #else
    pmem_memcpy_persist(dst , value.data() , value.size());
    pmem_memcpy_persist(&new_head , &head , sizeof(head_info));
    #endif

    index.insert(hash % HASH_SIZE , new_index_info(bucket_id) , key_index , prefix);

    return Ok;
}

void NvmEngine::recovery(){

    Logger::instance().sync_log("recovery...");
    std::array< std::thread  , BUCKET_CNT> ts;
    
    using max_off_array_t = std::array<uint32_t , BUCKET_CNT> ;
    max_off_array_t final_off{};
    std::vector<max_off_array_t> grid{ BUCKET_CNT , final_off}; 
    uint i = 0;
    for( auto & t : ts ) {
        t = std::thread{
            [this , &grid ,  i]{
                auto bk_id = get_bucket_id();
                //read head and build index
                // uint32_t max_off = 0 , len = 0;
                for(;;){
                    auto key_index = new_key_info(bk_id);
                    auto & head = file.key_heads[key_index];

                    if(head.len == 0){
                        -- bucket_infos[bk_id].key_seq;
                        break;
                    }

                    constexpr uint32_t num_block = decltype(allocator)::value_type::num_block;

                    uint32_t bk = head.beg / num_block;
                    uint32_t off = head.beg - bk * num_block + head.len / sizeof(value_block) + 1;

                    grid[i][bk] = std::max(grid[i][bk] , off);
                    
                    auto index_id = new_index_info(bk_id);

                    using prefix_t = decltype(decltype(index)::index_info{}.prefix);
                    auto prefix = * reinterpret_cast<const prefix_t *>(head.key); 
                    auto hash = hash_bytes_16(head.key);
                    index.insert( hash % HASH_SIZE , index_id , key_index , prefix);   
                }
                //optional : init from bitmap
            }
        };
        ++i;
    }

    for(auto & t : ts){
        t.join();
    }

    //merge
    for(const auto & off_arr : grid ){
        for(uint32_t i = 0 ; i< final_off.size() ; ++i){
            final_off[i] = std::max(final_off[i] , off_arr[i]);
        }
    }

    //retrive offset with some waste
    for(uint32_t i = 0 ; i < BUCKET_CNT ; ++i ){
        allocator[i].allocate(final_off[i]);
    }
}

NvmEngine::~NvmEngine() {
    pmem_unmap(file.base() , NVM_SIZE);
}
