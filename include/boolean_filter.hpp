#ifndef BOOLEAN_FILTER_INCLUDE_H
#define BOOLEAN_FILTER_INCLUDE_H

#include <atomic>
#include <array>

#include "include/utils.hpp"

template<std::size_t N>
class bitmap_filter:disable_copy{
    struct inner_block{
        std::atomic<uint8_t> value;

        constexpr explicit inner_block()noexcept
        :value(0){}

        bool test(std::size_t off) const{
            return value & (1 << off);
        }

        void set(std::size_t off) {
            value.fetch_or(1 << off);
        }
    };
public:
    static constexpr std::size_t max_index = ( N % 8 == 0 ? N : (N / 8 + 1) * 8);
    static_assert(max_index % 8 == 0 , "incorrect max_index .");
public:
    explicit bitmap_filter()
    :_bitset(){
    }

    bool test(std::size_t i ) const{
        // if(i >= max_index) return false;
        return _bitset[i >> 3].test(i & 0x07);
    }

    void set(std::size_t i){
        // if(i >= max_index) return ;
        _bitset[i >> 3].set(i & 0x07);
    }
    
private:
    static constexpr std::size_t max_size = max_index / 8;
    std::array<inner_block , max_size> _bitset;
};

#endif