#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>

#include <cstdio>
#include <cstdlib>
#include <string>

#include "logger.hpp"

off_t file_len(const std::string & file){
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

    ~memory_pool() noexcept{
        if(!_base)munmap(_base , N);
        if(!(_fd < 0)) close(_fd);
    }

    memory_pool(memory_pool && mem) noexcept
    :_base(mem._base){
        mem._base = nullptr;
    }

    void * init_memory(){
        Logger::instance().sync_log("file len = 0.");
        return mmap(NULL , N , PROT_READ|PROT_WRITE , MAP_ANON | MAP_SHARED , 0 ,0);
    }

    void * init_mmap_file(const std::string & file){

        Logger::instance().sync_log("file len = " + std::to_string((size_t)file_len(file)));

        //open
        _fd = open(file.data(), O_RDWR|O_CREAT, S_IRWXU);
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

    constexpr size_t length() const{
        return N;
    }

    void * base() const{
        return _base;
    }

protected:
    void *  _base{nullptr};
    std::atomic<size_t>  _endoff{0};
    int     _fd{-1};
};
