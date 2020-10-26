#ifndef OPEN_ADDRESS_HASH_INCLUDE_H
#define OPEN_ADDRESS_HASH_INCLUDE_H

#include <memory>
#include <atomic>
#include <iostream>

template<uint32_t N>
class open_address_hash{
public:
    static constexpr uint32_t n_bucket = N;
    static constexpr uint32_t null_id = 0xffffffff;

public:

    explicit open_address_hash() noexcept{
        using bucket_type = uint32_t;
        auto p = new bucket_type[N];
        memset(p , 0xff , sizeof(bucket_type) * N );

        bucket.reset(reinterpret_cast<std::atomic<uint32_t>*>(p));
    }

    void insert(uint64_t hash , uint32_t key_index ){
        for (uint32_t i = hash % N ,cnt = 0 ; cnt < N ; ++cnt ,++i , i %= N){
            if(bucket[i] != null_id) continue;

            uint32_t empty_val {null_id};
            if(bucket[i].compare_exchange_weak(
                empty_val, 
                key_index , 
                std::memory_order_release ,
                std::memory_order_relaxed))
                break;
        }
    }

    template<class F>
    uint32_t search(uint64_t hash , F && key_cmp_eq){
        for(uint32_t i = hash % N , cnt = 0 ; cnt < N ; ++ cnt , ++i , i %= N ){
            if(bucket[i] == null_id) break;
            if(key_cmp_eq(bucket[i]))
                return bucket[i];
        }
        return null_id;
    }

private:
    std::unique_ptr<std::atomic<uint32_t>[]> bucket{};
};

#endif