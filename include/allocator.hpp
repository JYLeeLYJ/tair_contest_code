#ifndef ALLOCATOR_INCLUDE_H
#define ALLOCATOR_INCLUDE_H

#include <array>
#include <vector>
#include <string>

#include "fmt/format.h"
#include "kvfile.hpp"

//not thread safe
template<std::size_t n_block>
class alignas(CACHELINE_SIZE) value_block_allocator{
public:
    static constexpr uint32_t null_index = 0xffffffff;
    static constexpr uint32_t total_block_num = n_block;
public:
    value_block_allocator() = default;

    void init(uint32_t beg , uint32_t off){
        this->beg = beg;
        this->off = off;

        free_block_128.reserve(7_MB);
        free_block_256.reserve(7_MB);
    }

    uint32_t allocate_128(){
        uint32_t addr{null_index};
        if(!free_block_128.empty()){ 
            addr = free_block_128.back();
            free_block_128.pop_back();
        }else if((addr = allocate_256())!= null_index){
            free_block_128.push_back(addr +1);
        }

        return addr ;
    }

    uint32_t allocate_256(){
        uint32_t addr{null_index};

        if(!free_block_256.empty()){
            addr = free_block_256.back();
            free_block_256.pop_back();
        }else if(off + 1 < n_block){
            addr = beg + off;
            off += 2;
        }
        return addr;
    }

    void recollect_128(uint32_t addr){
        free_block_128.push_back(addr);
    }

    void recollect_256(uint32_t addr){
        free_block_256.push_back(addr);
    }
    
    std::string space_use_log(){
        return fmt::format("[{} , {} , remains {}]" , free_block_128.size() , free_block_256.size() , n_block - off );
    }

private:

    uint32_t beg{0};
    uint32_t off{0};

    std::vector<uint32_t> free_block_128;
    std::vector<uint32_t> free_block_256;

};

#endif