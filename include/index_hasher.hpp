#ifndef INDEX_HASHER_INCLUDE_H
#define INDEX_HASHER_INCLUDE_H

#include <atomic>
#include <vector>
#include <limits>
#include <array>

#include "include/utils.hpp"
#include "fmt/format.h"

//HashMap with no rehashing
template<std::size_t bucket_size , std::size_t bucket_length>
class Hash{
public :
    struct bucket_head : disable_copy{
        std::atomic<uint8_t> value_cnt;
        uint8_t recent_vis_index;

        constexpr bucket_head() noexcept
        :value_cnt(0) , recent_vis_index(std::numeric_limits<uint8_t>::max()){}
    };

    struct bucket{
        uint32_t indics[bucket_length]{};
        // std::array<uint32_t , bucket_length> indexs{};
    };

    static constexpr std::size_t bk_size = bucket_size ;
    static constexpr std::size_t bk_len = bucket_length;

    static_assert(sizeof(bucket_head) == 2 , "error head size");
    static_assert(sizeof(bucket) == 32 , "error bucket len");

public:
    explicit Hash() noexcept
    :_buckets(bucket_size) , _heads(bucket_size){
    }
    
    std::pair<bucket_head &, bucket &> get_bucket(std::size_t i ){
        return {_heads[i] , _buckets[i]};
    }

    bool append(std::size_t i_bucket , uint32_t index) {
        bucket_head & head = _heads[i_bucket];
        bucket & bk = _buckets[i_bucket];
        uint8_t cnt = head.value_cnt++;
        if (cnt >= bucket_length){
            head.value_cnt = bucket_length; 
            return false;
        }
        head.recent_vis_index = cnt;
        bk.indics[cnt] = index;
        return true;
    }

    std::string print_bucket(std::size_t i) const {
        auto & bk = _buckets[i];
        return fmt::format("bucket index = {} , value_cnt = {} ,values={} {} {} {} {} {} {} {}" , i ,_heads[i].value_cnt.load(), bk.indics[0] , bk.indics[1] , bk.indics[2],
        bk.indics[3] ,bk.indics[4] , bk.indics[5] , bk.indics[6] , bk.indics[7] );
    }

private:

    std::vector<bucket> _buckets;
    std::vector<bucket_head> _heads;

};

#endif