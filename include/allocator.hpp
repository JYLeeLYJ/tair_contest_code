#ifndef ALLOCATOR_INCLUDE_H
#define ALLOCATOR_INCLUDE_H

#include <array>
#include <vector>
#include "kvfile.hpp"

//not thread safe
template<std::size_t n_block>
class alignas(CACHELINE_SIZE) value_block_allocator{
public:
    static constexpr uint32_t null_index = 0xffffffff;
public:
    explicit value_block_allocator(uint32_t off_ = 0) noexcept 
    :off (off_){
    }

    uint32_t allocate(std::size_t n) {
        uint32_t addr {null_index};


        if(n <= 8 && !free_space[n].empty()){
            addr = free_space [n].back();
            free_space[n].pop_back();
        }
        else if(off + n <= n_block){
            addr = off;
            off += n;
        }

        return addr;
    }

    void deallocate(std::size_t n , uint32_t beg) {
        free_space[n].push_back(beg);
    }

private:
    //using 1 ~ 8 , denote 128 ~ 1024
    std::array<std::vector<uint32_t> , 10> free_space{};
    uint32_t off ;
};

#endif