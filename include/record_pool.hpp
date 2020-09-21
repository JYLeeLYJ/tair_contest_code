#ifndef RECORD_POOL_INCLUDE_H
#define RECORD_POOL_INCLUDE_H

#include <utility>

#include "include/db.hpp"
#include "include/memory_pool.hpp"
#include "include/utils.hpp"
#include "include/logger.hpp"

struct Record{
    static constexpr std::size_t key_size = 16 ;
    static constexpr std::size_t value_size = 80;

    char _key[key_size]{};
    char _value[value_size]{};

    Record & set (const Slice & key , const Slice& value) noexcept{
        memcpy(_key , key.data() , key.size());
        memcpy(_value , value.data() , value.size());
        return *this;
    }

    Record & update_value(const Slice & value) noexcept{
        memcpy(_value , value.data() , value.size());
        return *this;
    }

    std::string key() const {
        return std::string(_key , key_size);
    }

    const char * key_data() const {
        return _key;
    } 

    std::string value() const{
        return std::string (_value , value_size);
    }
};

static_assert(sizeof(Record) == 96 , "error record size");

template<std::size_t N , std::size_t pool_bucket_cnt>
class record_pool{

    static_assert(N % pool_bucket_cnt == 0 , "error pool_bucket_cnt with N");
    static constexpr std::size_t pool_bucket_size = N / pool_bucket_cnt;

    //align atomic uint type
    struct alignas(64) align_atomic_uint64 
    : public std::atomic<uint64_t>{
        uint64_t operator = (uint64_t i){
            return std::atomic<uint64_t>::operator=(i);
        }
    };

public:
    explicit record_pool(const std::string & file_name) 
    : pool(file_name){
        // for(auto & s : seq)
        //     s= 0;
        Logger::instance().sync_log("record_pool init.");
    }

    Record & get_value(std::size_t index) const noexcept{
        return base()[index];
    }

    bool set_value(std::size_t index , const Slice & key , const Slice & value) noexcept{
        if(index >= N) 
            return false;

        auto & record = get_value(index);
        record.set(key , value);
        return true;
    }

    std::size_t allocate_seq(uint64_t key) noexcept {
        // auto bk = key % pool_bucket_cnt ;
        // auto index = seq[bk] ++ ;
        // if(index >= pool_bucket_size){
        //     seq[bk] = pool_bucket_size;
        //     return std::numeric_limits<std::size_t>::max();
        // }
        // else 
        //     return index + bk * pool_bucket_size;
        auto index = seq++;
        return index >= N ? std::numeric_limits<std::size_t>::max() : index;
    }

    static inline bool is_valid_index(const std::size_t i) {
        return i != std::numeric_limits<std::size_t>::max();
    }

private:
    memory_pool<N * sizeof(Record)> pool;
    alignas(64) std::atomic<std::size_t> seq{0};
    // alignas(64) std::array<align_atomic_uint64, pool_bucket_cnt> seq;

    // static_assert(sizeof(seq) % 64 == 0 , "error align atomic value.");

    Record * base() const noexcept{
        return static_cast<Record *>(pool.base());
    }
};

#endif