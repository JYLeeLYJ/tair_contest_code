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

#include "include/utils.hpp"

// #define LOCAL_TEST

template<size_t N>
class memory_pool:disable_copy{
public:
    explicit memory_pool(const std::string file){

        #ifdef LOCAL_TEST
        _base = (char*)mmap(NULL,N , PROT_READ | PROT_WRITE, MAP_ANON | MAP_SHARED , 0,0);
        #else 
        _fd = open(file.c_str(), O_RDWR);
        if (_fd < 0) {
            perror("failed to open the file");
            exit(1);
        }
        _base = (char*)mmap(NULL, N, PROT_READ | PROT_WRITE, MAP_SHARED, _fd, 0);
        #endif

        if(_base == MAP_FAILED || _base == NULL) {
            perror("mmap failed");
            exit(1);
        }
    }

    ~memory_pool() noexcept{
        if(!_base)munmap(_base , N);
        #ifndef LOCAL_TEST
        close(_fd);
        #endif
    }

    memory_pool(memory_pool && mem) noexcept
    :_base(mem._base){
        mem._base = nullptr;
    }

    memory_pool & operator= (memory_pool && mem) noexcept{
        _base = mem._base; 
        mem._base = nullptr;
    }

    constexpr size_t length() const{
        return N;
    }

    char * base() const{
        return _base;
    }

protected:
    char * _base{nullptr};
    int _fd{-1};
};

struct Value{

    static constexpr size_t value_size = 80;

    char data[value_size]{};
    
    std::string to_string() const {
        return std::string(&data[0] , value_size);
    }

    Value & operator = (const std::string & str) noexcept{
        memset(data , 0 , value_size);
        memcpy(data , str.data() , std::min(str.length() , value_size));
        return * this;
    }
};

template<size_t N>
class value_pool:disable_copy{
public:
    static constexpr size_t VALUE_SIZE = Value::value_size; 
    static constexpr size_t KEY_SIZE = 16;
public:
    explicit value_pool(const std::string & file ) :pmem(file){}

    //out of range will lead to undefined behavior
    // std::string operator[] (size_t i) const noexcept{
    //     return std::string(pmem.base() + i * VALUE_SIZE  , VALUE_SIZE);
    // }

    // bool write_value(size_t i , const std::string & str) noexcept{
    //     if (i >= N)
    //         return false;
    //     else {
    //         auto offset = i * VALUE_SIZE;
    //         memset(pmem.base() + offset, 0 , VALUE_SIZE );
    //         memcpy(pmem.base() + offset, str.data() , std::min(VALUE_SIZE,str.length()));
    //         return true;
    //     }
    // }

    // std::string get_value(size_t i) const noexcept{
    //     return std::string(pmem.base() + i * VALUE_SIZE  , VALUE_SIZE);
    // }

    //out of range will lead to undefined behavior
    Value & operator[](size_t i) const noexcept{
        return value(i);        
    }

    Value & value(size_t i) const noexcept{
        return data()[i];
    }

    bool set_value(size_t i , const std::string & str) noexcept{
        if (i > N)
            return false;
        else{
            data()[i] = str;
            return true;
        }
    }

private:
    memory_pool<N * VALUE_SIZE> pmem;

    Value * data() const noexcept{
        return reinterpret_cast<Value*>(pmem.base());
    }
};

#endif