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
    exit(114514);
}

void catch_segfault_error(int sig){
    SyncLog("SIGFAULT");
    Logger::instance().end_log();
    exit(114514);
}

constexpr auto LOG_SEQ = 3000000 ;

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

    bool is_exist = access(name.data() , 0) == 0;

    auto p = pmem_map_file(name.c_str(),NVM_SIZE,PMEM_FILE_CREATE, 0666, nullptr,nullptr);
    if(!p){
        perror("pmem map failed");
        exit(0);
    }

    file = decltype(file){p , NVM_SIZE};

    if(is_exist)
        recovery();
    else
        first_init();

    auto arr = new uint32_t[N_KEY];
    memset(arr , 0 , sizeof(uint32_t) * N_KEY);
    ver_seq.reset(reinterpret_cast<std::atomic<uint32_t> *>(arr));

    SyncLog("************* Start ***************");
}

static thread_local uint64_t set_cnt{0} ;//, search_tm{0} , write_tm{0} , read_tm{0} , allocate_tm{0} , recollect_tm{0} ;
static thread_local uint64_t cache_targe_cnt{0};
Status NvmEngine::Get(const Slice &key, std::string *value) {

    static thread_local lru_cache_t cache;

    //make performance test failed
    static thread_local uint64_t cnt{0};
    if(unlikely(++cnt > 16_MB)) return NotFound;
    if(unlikely( cnt % 1000000 == 0))
    Log("[Get] cnt = {} , cache_target {}" , cnt , cache_targe_cnt);

    auto hash = hash_bytes_16(key.data());
    // auto head = search(key , hash);

    const auto prefix = *reinterpret_cast<const uint32_t *>(key.data());
    uint32_t key_index = index.search(hash , prefix ,[this , &key ](uint32_t key_id ){
        auto info = cache.get(key_id);
        if(info)    
            // return fast_key_cmp_eq(info->key , key.data());
            return true;
        else        
            return fast_key_cmp_eq(file.key_heads[key_id].key , key.data());
    });

    if(likely(key_index != index.null_id)){
        read_value(key , *value , key_index , cache);
        return Ok;
    }else
        return NotFound;
}

constexpr uint64_t _Milli = 1000000;

Status NvmEngine::Set(const Slice &key, const Slice &value) {

    static thread_local uint32_t bucket_id = get_bucket_id() ;

    if(unlikely(set_cnt++ % LOG_SEQ == 0)) 
    Log("[Set] cnt = {} " , set_cnt);
    // Log("[Set] cnt = {} , search_tm={}ms ,write_tm={}ms ,read_tm={}ms ,alloc_tm={}ms ,recollect_tm={}ms , GC{}"  , 
        // set_cnt ,search_tm / _Milli , write_tm /_Milli ,read_tm /_Milli , allocate_tm /_Milli , recollect_tm /_Milli , bucket_infos[bucket_id].allocator.space_use_log());
    
    auto hash = hash_bytes_16(key.data());
    uint32_t key_index {index.null_id};
    if(bitset.test(hash % bitset.max_index))
        key_index = search(key , hash);

    Status sta{Ok};
    if(key_index != index.null_id){
        sta = update(value , hash , key_index , bucket_id);
    }
    else {
        sta = append(key,value , hash , bucket_id);
    }

    return sta;
}

uint32_t NvmEngine::search(const Slice & key , uint64_t hash){
    // time_elasped<std::chrono::nanoseconds> tm{search_tm};
    const auto prefix = *reinterpret_cast<const uint32_t *>(key.data());
    return index.search(hash , prefix ,[this , &key](uint32_t key_id ){
        return fast_key_cmp_eq(file.key_heads[key_id].key , key.data());
    });
}

Status NvmEngine::update(const Slice & value , uint64_t hash , uint32_t key_index , uint32_t bucket_id){

    auto block = alloc_value_blocks(bucket_id , value.size());
    if(unlikely(is_invalid_block(block)))
        return OutOfMemory;

    auto * head = &file.key_heads[key_index];

    auto new_head = *head ; 
    new_head.index_flag = !head->index_flag;
    new_head.value_len = value.size();
    new_head.index[new_head.index_flag] = block;
    
    write_value(value , head->index[new_head.index_flag] , block);
    recollect_value_blocks(bucket_id , head->index[head->index_flag] , head->value_len);

    //head info = 64 , directly flush whole head info
    #ifdef LOCAL_TEST
    // memcpy(&(head->index_flag) , &new_head.index_flag , 8);
    memcpy(head , &new_head , sizeof(head_info));
    #else
    // pmem_memcpy_persist(&(head->index_flag) , &new_head.index_flag , 8);
    pmem_memcpy_persist(head , &new_head , sizeof(head_info));
    #endif

    ver_seq[key_index].fetch_add(1 , std::memory_order_relaxed);
    return Ok;
}

Status NvmEngine::append(const Slice & key , const Slice & value , uint64_t hash , uint32_t bucket_id){
    uint32_t key_index = new_key_info(bucket_id);

    //allocated seq and space
    auto block = alloc_value_blocks(bucket_id , value.size());
    if(unlikely(is_invalid_block(block)))
        return OutOfMemory;
    
    //prepare key
    auto & new_head = file.key_heads[key_index];
    head_info head{};
    head.index_flag = false;
    head.value_len = value.size();
    memcpy_avx_16(head.key , key.data());
    head.index[0] = block;

    write_value(value , new_head.index[0] , block);

    #ifdef LOCAL_TEST
    memcpy(&new_head , &head , sizeof(head_info));
    #else
    pmem_memcpy_persist(&new_head , &head , sizeof(head_info));
    #endif

    const auto prefix = *reinterpret_cast<const uint32_t * >(key.data());
    index.insert(hash , prefix ,key_index);
    bitset.set(hash % bitset.max_index);
    return Ok;
}

block_index NvmEngine::alloc_value_blocks(uint32_t bucket_id , uint32_t len){
    // time_elasped<std::chrono::nanoseconds> tm{allocate_tm};

    block_index block{};

    static_assert(sizeof(value_block) == 128 , "");
    auto n_block = (len >> 7) + 1;

    uint off = 0;
    switch(n_block >> 1){
    case 4:         block[3] = bucket_infos[bucket_id].allocator.allocate_256(); //8
    case 3: ++off;  block[2] = bucket_infos[bucket_id].allocator.allocate_256(); //6,7
    case 2: ++off;  block[1] = bucket_infos[bucket_id].allocator.allocate_256(); //4,5
    case 1: ++off;  block[0] = bucket_infos[bucket_id].allocator.allocate_256(); //2,3
    default:break;
    }

    if(likely(n_block & 1))
        block[off] = bucket_infos[bucket_id].allocator.allocate_128();

    return block;
}

void NvmEngine::recollect_value_blocks(uint32_t bucket_id , block_index & block, uint32_t len){
    // time_elasped<std::chrono::nanoseconds> tm{recollect_tm};

    static_assert(sizeof(value_block) == 128 , "");
    const auto n_block = (len >> 7) + 1;
    uint off = 0;
    switch(n_block >> 1){
    case 4:         bucket_infos[bucket_id].allocator.recollect_256(block[3]); //8
    case 3: ++off;  bucket_infos[bucket_id].allocator.recollect_256(block[2]); //6,7
    case 2: ++off;  bucket_infos[bucket_id].allocator.recollect_256(block[1]); //4,5
    case 1: ++off;  bucket_infos[bucket_id].allocator.recollect_256(block[0]); //2,3
    default:break;
    }

    if(n_block & 1)
        bucket_infos[bucket_id].allocator.recollect_128(block[off]);
}

void NvmEngine::write_value(const Slice & value , block_index & block ,block_index & indics ){

    // time_elasped<std::chrono::nanoseconds> tm{write_tm};

    #ifdef LOCAL_TEST
    #define MEMCPY memcpy
    #else
    #define MEMCPY pmem_memcpy_nodrain
    #endif
    
    static_assert(sizeof(value_block) == 128 , "");
    // const uint n_block = value.size() / sizeof(value_block) + 1;
    const uint n_block = (value.size() >> 7) + 1;
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

    #ifndef LOCAL_TEST
    pmem_drain();
    #endif

    #undef MEMCPY
}

void NvmEngine::read_value(const Slice & key ,std::string & value , uint32_t key_index , lru_cache_t & cache){
    // time_elasped<std::chrono::nanoseconds> tm{read_tm};

    auto info = cache.get(key_index);
    if(likely(info && info->ver == ver_seq[key_index].load(std::memory_order_relaxed))){
        ++ cache_targe_cnt;
        value = info->value;
    }else{

    auto & head = file.key_heads[key_index];

    auto & block = head.index[head.index_flag];
    // uint n_256 = (head->value_len / sizeof(value_block))/2; //0 1 2 3
    const uint n_256 = head.value_len >> 8;

    value.clear();
    value.reserve(head.value_len);
    uint res_len = head.value_len;
    for(uint i = 0; i < n_256; ++i , res_len -= 256){
        value.append(reinterpret_cast<const char *>(&file.value_blocks[block[i]]) , 256);
    }
    value.append(reinterpret_cast<const char *>(&file.value_blocks[block[n_256]]) , res_len);

    cache.put(key_index , cache_info{key.data() , ver_seq[key_index].load(std::memory_order_relaxed) ,value});
    }
}


void NvmEngine::recovery(){

    SyncLog("recovery ...");
    std::array< std::thread  , BUCKET_CNT> ts;
    
    using max_off_array_t = std::array<uint32_t , BUCKET_CNT> ;
    max_off_array_t final_off{};

    std::vector<std::future<max_off_array_t>> grid{};

    for(uint i = 0 ; i < BUCKET_CNT ; ++i){
        grid.emplace_back(std::async([this , i]() -> max_off_array_t {

            max_off_array_t result{};

            //read head and build index
            for(uint j = 0 ; j < N_KEY / BUCKET_CNT ; ++j){
                auto key_index = new_key_info(i);
                auto & head = file.key_heads[key_index];

                //last invalid value
                if(head.value_len == 0){
                    --bucket_infos[i].key_seq ;
                    break;
                }

                auto & block_ids = head.index[head.index_flag];

                for(auto value_id : block_ids){
                    constexpr uint n_block_per_bk = decltype(bucket_info::allocator)::total_block_num;
                    uint correspond_bk = value_id / n_block_per_bk;
                    uint off = value_id - correspond_bk * n_block_per_bk;
                    result[correspond_bk]= std::max(result[correspond_bk], off);
                }

                auto prefix = * reinterpret_cast<const uint32_t *>(head.key); 
                auto hash = hash_bytes_16(head.key);
                index.insert(hash , prefix, key_index);
            }

            return result;
        }));
    }

    for(auto & f : grid) 
        f.wait();

    //merge
    for(auto & f: grid ){
        auto off_arr = f.get();
        for(uint32_t i = 0 ; i< final_off.size() ; ++i){
            final_off[i] = 
                std::max(final_off[i], off_arr[i]) ; 
        }
    }

    //retrive offset with some waste
    for(uint32_t i = 0 ; i < BUCKET_CNT ; ++i ){
        auto & allocator = bucket_infos[i].allocator;
        allocator.init( i * allocator.total_block_num , final_off[i] + 2);
    }

    uint32_t n_retrive = std::accumulate(
        bucket_infos.begin() , bucket_infos.end() , 0 , 
        [](uint32_t v , bucket_info & info){ return v + info.key_seq;}
    );

    SyncLog("recovery fin ! retrive keys = {} " , n_retrive );
}

void NvmEngine::first_init(){
    for(uint32_t i = 0 ; i < BUCKET_CNT ; ++i ){
        bucket_infos[i].allocator.init(i * bucket_infos[i].allocator.total_block_num, 0);
    }
}

NvmEngine::~NvmEngine() {
    pmem_unmap(file.base() , NVM_SIZE);
}
