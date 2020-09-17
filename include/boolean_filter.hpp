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
            uint8_t v = value;
            while(!value.compare_exchange_weak(
                v , static_cast<uint8_t>(v | (1 << off)) , std::memory_order_release , std::memory_order_relaxed
            ));
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
        if(i >= max_index) return false;
        std::size_t n = i / 8 , off = i % 8;
        return _bitset[n].test(off);
    }

    void set(std::size_t i){
        if(i >= max_index) return ;
        std::size_t n = i / 8 , off = i % 8;
        _bitset[n].set(off);
    }
    
private:
    static constexpr std::size_t max_size = max_index / 8;
    std::array<inner_block , max_size> _bitset;
};

#endif