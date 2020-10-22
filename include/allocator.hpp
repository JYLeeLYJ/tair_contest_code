#ifndef ALLOCATOR_INCLUDE_H
#define ALLOCATOR_INCLUDE_H

#include <array>
#include <vector>
#include <string>
#include "kvfile.hpp"

//not thread safe
template<std::size_t n_block>
class alignas(CACHELINE_SIZE) value_block_allocator{
public:
    static constexpr uint32_t null_index = 0xffffffff;
    static constexpr uint32_t num_block = n_block;
public:
    value_block_allocator() = default;

    void reinit(uint32_t beg_ , uint32_t off_) noexcept{
        beg = beg_; 
        off = off_;
    }

    uint32_t allocate(std::size_t n) {
        uint32_t addr {null_index};

        if(n <= 8 && !free_space[n].empty()){
            addr = free_space [n].back();
            free_space[n].pop_back();
        }
        else if(off + n <= n_block){
            addr = beg + off;
            off += n;
        }

        return addr;
    }


    void deallocate(std::size_t n , uint32_t beg) {
        free_space[n].push_back(beg);
    }

    std::string print_space_use(){
        std::string str{};
        for(uint i = 1; i<=8 ; ++ i){
            str.append(std::to_string(free_space[i].size())).append(" ");
        }
        return str.append("| ").append(std::to_string(n_block - off));
    }

private:
    //using 1 ~ 8 , denote 128 ~ 1024
    std::array<std::vector<uint32_t> , 10> free_space{};
    uint32_t beg{0};
    uint32_t off{0};
};

#endif