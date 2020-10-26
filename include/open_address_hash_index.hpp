#ifndef OPEN_ADDRESS_HASH_INCLUDE_H
#define OPEN_ADDRESS_HASH_INCLUDE_H

#include <memory>
#include <atomic>
#include <numeric>

template<uint32_t N>
class open_address_hash{
public:
    static constexpr uint32_t n_bucket = N;
    static constexpr uint32_t null_id = 0xffffffff;

private:

    using bucket_type = uint64_t;
    static constexpr uint64_t null_bucket = std::numeric_limits<uint64_t>::max();

public:

    explicit open_address_hash() noexcept{
        auto p = new bucket_type[N];
        memset(p , 0xff , sizeof(bucket_type) * N );

        bucket.reset(reinterpret_cast<std::atomic<bucket_type>*>(p));
    }

    void insert(uint64_t hash , uint32_t prefix , uint32_t key_index){

        union {
            std::pair<uint32_t , uint32_t > info{} ;
            uint64_t n ;
        };

        info = {prefix , key_index};

        for (uint32_t i = hash % N ,cnt = 0 ; cnt < N ; ++cnt ,++i , i %= N){
            if(bucket[i] != null_bucket) continue;

            uint64_t empty_val {null_bucket};
            if(bucket[i].compare_exchange_weak(
                empty_val, n,
                std::memory_order_release ,
                std::memory_order_relaxed))
                break;
        }

    }

    template<class F>
    uint32_t search(uint64_t hash , uint32_t prefix , F && key_cmp_eq){
        union {
            std::pair<uint32_t , uint32_t > info{} ;
            uint64_t n ;
        };

        for(uint32_t i = hash % N , cnt = 0 ; cnt < N ; ++ cnt , ++i , i %= N ){
            if(bucket[i] == null_bucket) break;
            n = bucket[i];
            if(info.first == prefix && key_cmp_eq(info.second))
                return info.second;
        }
        return null_id;
    }

private:
    std::unique_ptr<std::atomic<bucket_type>[]> bucket{};
};

#endif