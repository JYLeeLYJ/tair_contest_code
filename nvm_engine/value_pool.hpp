#ifndef VALUE_POOL_INCLUDE_H
#define VALUE_POOL_INCLUDE_H

#include <sys/mman.h>
#include <cstdio>
#include <cstdlib>
#include <utility>
#include <string>
#include <cstring>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>

#include "include/utils.hpp"
#include "include/db.hpp"
#include "include/memory_pool.hpp"

struct Value{

    static constexpr size_t value_size = 80;

    char data[value_size]{};
    
    std::string to_string() const {
        return std::string(&data[0] , value_size);
    }

    Value & operator = (const Slice & val) noexcept{
        memset(data , 0 , value_size);
        memcpy(data , val.data() , std::min(val.size(), value_size));
        return * this;
    }
};

template<size_t N , bool pre_allocate = false>
class value_pool:disable_copy{
public:
    static constexpr size_t VALUE_SIZE = Value::value_size; 
    static constexpr size_t KEY_SIZE = 16;
    static constexpr size_t MEM_SIZE = VALUE_SIZE * N;
public:
    explicit value_pool(const std::string & file ) 
    :pmem(file){
    }

    //out of range will lead to undefined behavior
    Value & operator[](size_t i) const noexcept{
        return value(i);        
    }

    Value & value(size_t i) const noexcept{
        return data()[i];
    }

    bool set_value(size_t i , const Slice & str) noexcept{
        data()[i] = str;
        return true;
    }

    size_t append_new_value(const Slice & str ) {
        auto index = seq ++;
        set_value(index , str);
        return index;
    }

private:

    Value * data() const noexcept{
        return static_cast<Value*>(pmem.base());
    }

private:
    memory_pool< MEM_SIZE > pmem;
    std::atomic<size_t> seq{0};

};

#endif