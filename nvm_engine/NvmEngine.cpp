#include "NvmEngine.hpp"
#include "include/utils.hpp"
#include "include/logger.hpp"
#include "fmt/format.h"

#include <tuple>
#include <algorithm>
#include <numeric>
#include <future>

#include <libpmem.h>
#include <sys/mman.h>
#include <unistd.h>
#include <signal.h>

#include <cassert>

void catch_bus_error(int sig){
    SyncLog("SIGBUS");
    exit(1);
}

void catch_segfault_error(int sig){
    SyncLog("SIGFAULT");
    Logger::instance().end_log();
    exit(1);
}

constexpr auto LOG_SEQ = 3000000;

Status DB::CreateOrOpen(const std::string &name, DB **dbptr, FILE *log_file) {
    Logger::instance().set_file(log_file);
    SyncLog("file : {} " , name);
    return NvmEngine::CreateOrOpen(name, dbptr);
}

DB::~DB() {}

Status NvmEngine::CreateOrOpen(const std::string &name, DB **dbptr) {

    signal(SIGBUS  , catch_bus_error);
    signal(SIGSEGV , catch_segfault_error);

    *dbptr = new NvmEngine(name);

    return Ok;
}

NvmEngine::NvmEngine(const std::string &name) {

    SyncLog("hardware concurrency = {}" , std::thread::hardware_concurrency());

    bool is_exist = access(name.data() , 0) == 0;

    auto p = pmem_map_file(name.c_str(),NVM_SIZE,PMEM_FILE_CREATE, 0666, nullptr,nullptr);
    if(!p){
        perror("pmem map failed");
        exit(0);
    }

    file = decltype(file){p , NVM_SIZE};

    // if(is_exist)
    //     recovery();
    // else{
        for(uint32_t i = 0 ; i < BUCKET_CNT ; ++i ){
            allocator[i].init(i * allocator[i].total_block_num, 0);
        }
    // }

    Logger::instance().sync_log("************* Start ***************");
}

Status NvmEngine::Get(const Slice &key, std::string *value) {

    static thread_local uint64_t cnt {0};
    // if(unlikely(cnt ++ % LOG_SEQ == 0))
    // Log("[Get] key = {} , value = {}" , value);

    auto hash = hash_bytes_16(key.data());
    auto info = search(key , hash);

    if(info){
        auto head = &file.key_heads[info->index];
        read_value(*value , head );
        // Log("[Got] value = {}" , *value);
        return Ok;
    }else{
        Log("not found.");
        return NotFound;
    }

    return NotFound;
}

Status NvmEngine::Set(const Slice &key, const Slice &value) {

    static thread_local uint32_t bucket_id = get_bucket_id() ;

    static thread_local uint64_t cnt{0} , ud_cnt{0} , ap_cnt{0};

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

    auto allocated_info = alloc_value_blocks(bucket_id , value.size());
    if(allocated_info.first == allocator[0].null_index)
        return OutOfMemory;

    auto new_head = *head ; 
    new_head.index_block_id = allocated_info.first;
    new_head.value_len = value.size();
    
    write_value(value , allocated_info.first , allocated_info.second);

    //exchange
    #ifdef LOCAL_TEST
    memcpy(head , &new_head , sizeof(head_info));
    #else
    pmem_memcpy_persist(head , &new_head , sizeof(head_info));
    #endif

    recollect_value_blocks(bucket_id , head->index_block_id , head->value_len);
    return Ok;
}

Status NvmEngine::append(const Slice & key , const Slice & value , uint64_t hash , uint32_t bucket_id){

    uint32_t key_index = new_key_info(bucket_id);
    if(unlikely(key_index == hash_t::null_id)){
        Logger::instance().sync_log("key pool is full!");
        return OutOfMemory;
    }

    //allocated seq and space
    auto allocated_info = alloc_value_blocks(bucket_id , value.size());
    if(allocated_info.first == allocator[0].null_index)
        return OutOfMemory;

    //prepare key
    auto & new_head = file.key_heads[key_index];
    head_info head;
    head.index_block_id = allocated_info.first;
    head.value_len = value.size();
    memcpy_avx_16(head.key , key.data());

    write_value(value , allocated_info.first , allocated_info.second);

    #ifdef LOCAL_TEST
    memcpy(&new_head , &head , sizeof(head_info));
    #else
    pmem_memcpy_persist(&new_head , &head , sizeof(head_info));
    #endif

    using prefix_t = decltype(decltype(index)::index_info{}.prefix);
    const auto prefix = *reinterpret_cast<const prefix_t * >(key.data());
    index.insert(hash % HASH_SIZE , new_hash_index_info(bucket_id) , key_index , prefix);

    return Ok;
}

std::pair<uint32_t , block_index> NvmEngine::alloc_value_blocks(uint32_t bucket_id , uint32_t len){
    std::pair<uint32_t , block_index> result{};
    
    auto & free_block_list = bucket_infos[bucket_id].free_index_blocks;

    uint32_t seq{allocator[0].null_index};
    if(!free_block_list.empty()){
        seq = free_block_list.back();
        free_block_list.pop_back();
    }else{
        constexpr auto n_index_block_per_bk = N_BLOCK_INDEX / BUCKET_CNT;
        seq = bucket_infos[bucket_id].block_index_seq ++ ;
        seq = seq < n_index_block_per_bk ? seq + bucket_id * n_index_block_per_bk : allocator[0].null_index;
    }

    result.first = seq;
    if(seq == allocator[0].null_index)
        return result;

    auto & block = result.second;// file.block_indices[seq];
    auto n_block = len / sizeof(value_block) + 1;

    uint off = 0;
    switch(n_block >> 1){
    case 4:         block[3] = allocator[bucket_id].allocate_256(); //8
    case 3: ++off;  block[2] = allocator[bucket_id].allocate_256(); //6,7
    case 2: ++off;  block[1] = allocator[bucket_id].allocate_256(); //4,5
    case 1: ++off;  block[0] = allocator[bucket_id].allocate_256(); //2,3
    default:break;
    }

    if(n_block & 1)
        block[off] = allocator[bucket_id].allocate_128();

    return result;
}

void NvmEngine::recollect_value_blocks(uint32_t bucket_id , uint32_t index_block_id , uint32_t len){
    auto & block = file.block_indices[index_block_id];
    
    auto n_block = len / sizeof(value_block) + 1;
    uint off = 0;
    switch(n_block >> 1){
    case 4:         allocator[bucket_id].recollect_256(block[3]); //8
    case 3: ++off;  allocator[bucket_id].recollect_256(block[2]); //6,7
    case 2: ++off;  allocator[bucket_id].recollect_256(block[1]); //4,5
    case 1: ++off;  allocator[bucket_id].recollect_256(block[0]); //2,3
    default:break;
    }

    if(n_block & 1)
        allocator[bucket_id].recollect_128(block[off]);

    bucket_infos[bucket_id].free_index_blocks.push_back(index_block_id);
}

void NvmEngine::write_value(const Slice & value  ,uint32_t index_block_id ,block_index & indics ){
    #ifdef LOCAL_TEST
    #define MEMCPY memcpy
    #else
    #define MEMCPY pmem_memcpy_persist
    #endif
    
    MEMCPY(&file.block_indices[index_block_id] , &indics , sizeof(block_index));
    
    static_assert(sizeof(value_block) == 128 , "");
    const uint n_block = value.size() / sizeof(value_block) + 1;
    uint off = 0;
    switch(n_block){
    case 8 :
    case 7 : ++off ; MEMCPY(&file.value_blocks[indics[2]] , value.data() + 512 , 256);
    case 6 :
    case 5 : ++off ; MEMCPY(&file.value_blocks[indics[1]] , value.data() + 256 , 256);
    case 4 : 
    case 3 : ++off ; MEMCPY(&file.value_blocks[indics[0]] , value.data() , 256);
    default:break;
    }

    uint res_len = value.size() & (n_block & 1 ? 127 : 255);
    MEMCPY(&file.value_blocks[indics[off]] , value.data() + off * 256 , res_len);

    // #ifndef LOCAL_TEST
    // pmem_drain();
    // #endif

    #undef MEMCPY
}

void NvmEngine::read_value(std::string & value , head_info * head){
    auto & block = file.block_indices[head->index_block_id];
    uint n_256 = (head->value_len / sizeof(value_block))/2; //0 1 2 3

    value.clear();
    value.reserve(head->value_len);
    uint res_len = head->value_len;
    for(uint i = 0; i < n_256; ++i , res_len -= 256){
        value.append(reinterpret_cast<const char *>(&file.value_blocks[block[i]]) , 256);
    }

    value.append(reinterpret_cast<const char *>(&file.value_blocks[block[n_256]]) , res_len);
}


void NvmEngine::recovery(){

    // SyncLog("recovery ...");
    // std::array< std::thread  , BUCKET_CNT> ts;
    
    // using max_off_array_t = std::vector<std::pair<uint32_t,uint32_t>> ;
    // max_off_array_t final_off{BUCKET_CNT};

    // std::vector<std::future<max_off_array_t>> grid{};

    // for(uint i = 0 ; i < BUCKET_CNT ; ++i){
    //     grid.emplace_back(std::async([this , i]() -> max_off_array_t {

    //         max_off_array_t result{BUCKET_CNT};

    //         //read head and build index
    //         for(uint j = 0 ; j < N_KEY / BUCKET_CNT ; ++j){
    //             auto key_index = new_key_info(i);
    //             auto & head = file.key_heads[key_index];

    //             //last invalid value
    //             if(head.value_len == 0){
    //                 --bucket_infos[i].key_seq ;
    //                 break;
    //             }

    //             auto & block_ids = file.block_indices[head.index_block_id];

    //             for(auto value_id : block_ids){
    //                 constexpr uint n_block_per_bk = decltype(allocator)::value_type::total_block_num;
    //                 uint correspond_bk = value_id / n_block_per_bk;
    //                 uint off = value_id - correspond_bk * n_block_per_bk;
    //                 result[correspond_bk].first = std::max(result[correspond_bk].first , off);
    //             }

    //             constexpr uint n_index_block_per_bk = N_BLOCK_INDEX / BUCKET_CNT;
    //             uint correspond_bk =  head.index_block_id / n_index_block_per_bk;
    //             uint off = head.index_block_id - correspond_bk * n_index_block_per_bk;
    //             result[correspond_bk].second = std::max(result[correspond_bk].second , off);
                
    //             auto index_id = new_hash_index_info(i);

    //             using prefix_t = decltype(decltype(index)::index_info{}.prefix);    //uint32_t
    //             auto prefix = * reinterpret_cast<const prefix_t *>(head.key); 
    //             auto hash = hash_bytes_16(head.key);
    //             index.insert( hash % HASH_SIZE , index_id , key_index , prefix);   
    //         }

    //         return result;
    //     }));
    // }

    // for(auto & f : grid) 
    //     f.wait();

    // //merge
    // for(auto & f: grid ){
    //     auto off_arr = f.get();
    //     for(uint32_t i = 0 ; i< final_off.size() ; ++i){
    //         final_off[i] = {
    //             std::max(final_off[i].first , off_arr[i].first) , 
    //             std::max(final_off[i].second, off_arr[i].second)};
    //     }
    // }

    // //retrive offset with some waste
    // for(uint32_t i = 0 ; i < BUCKET_CNT ; ++i ){
    //     bucket_infos[i].block_index_seq = final_off[i].first + 1;
    //     allocator[i].init( i * allocator[i].total_block_num , final_off[i].second + 2);
    // }

    // uint32_t n_retrive = std::accumulate(
    //     bucket_infos.begin() , bucket_infos.end() , 0 , 
    //     [](uint32_t v , bucket_info & info){ return v + info.key_seq;}
    // );

    // SyncLog("recovery fin ! retrive_key = {} " , n_retrive);
}

NvmEngine::~NvmEngine() {
    pmem_unmap(file.base() , NVM_SIZE);
}
