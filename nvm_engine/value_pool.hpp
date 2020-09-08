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
#include "include/logger.hpp"

inline off_t file_len(const std::string & file){
    struct stat file_stat{};
    if(stat(file.data() , &file_stat) == 0)
        return file_stat.st_size;
    else 
        return 0;
}

template<size_t N >
class memory_pool:disable_copy{
public:
    explicit memory_pool(const std::string & file){

        Logger::instance().sync_log("file = "+file);
        
        _base = init_mmap_file(file);

        if(_base == MAP_FAILED || _base == NULL) {
            Logger::instance().sync_log("failed to mmap.");
            perror("mmap failed");
            exit(1);
        }

        Logger::instance().sync_log("mmap base addr = " + std::to_string(uint64_t(_base)));
    }

    ~memory_pool() {
        if(!_base)munmap(_base , N);
        if(!(_fd < 0)) close(_fd);
    }

    memory_pool(memory_pool && mem) noexcept
    :_base(mem._base){
        mem._base = nullptr;
    }

    void * init_mmap_file(const std::string & file){

        Logger::instance().sync_log("file len = " + std::to_string((size_t)file_len(file)));
        
        //open
        _fd = open(file.data(), O_RDWR | O_CREAT ,S_IRWXU);
        if (_fd < 0) {
            Logger::instance().sync_log("failed to open the file");
            perror("failed to open the file");
            exit(1);
        }

        //pre allocate
        if(posix_fallocate(_fd , 0 , N) < 0){
            Logger::instance().sync_log("fallocate space failed." + std::string(strerror(errno)) );
            perror("fallocate failed");
            exit(1);
        }

        _endoff = N;

        //mmap
        return mmap(NULL, N, PROT_READ | PROT_WRITE, MAP_SHARED, _fd, 0);
    }
    
    memory_pool & operator= (memory_pool && mem) noexcept{
        _base = mem._base; 
        mem._base = nullptr;
    }

    constexpr size_t length() const noexcept{
        return N;
    }

    void * base() const noexcept{
        return _base;
    }

protected:
    void *  _base{nullptr};
    int     _fd{-1};

    std::atomic<size_t>  _endoff{0};
};

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

template<size_t N>
class value_pool:disable_copy{
public:
    static constexpr size_t VALUE_SIZE = Value::value_size; 
    static constexpr size_t KEY_SIZE = 16;
    static constexpr size_t MEM_SIZE = VALUE_SIZE * N;
public:
    explicit value_pool(const std::string & file ) 
    :pmem(file){
        Logger::instance().sync_log("value_pool init.");
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

    size_t append_new_value(const Slice & str ) noexcept {
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