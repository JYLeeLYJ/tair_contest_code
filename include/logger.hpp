#ifndef LOGGER_INCLUDE_H
#define LOGGER_INCLUDE_H

#include <string>
#include <mutex>
#include <queue>
#include <atomic>
#include <condition_variable>
#include <thread>
#include <chrono>

#include "utils.hpp"

#include "fmt/format.h"
#include "fmt/printf.h"

class Logger:disable_copy{

    using min_t = std::chrono::duration<float , std::ratio<60>>;
    using sec_t = std::chrono::duration<float , std::ratio<1>>;

public:
    static Logger & instance(){
        static Logger log{};
        return log;
    }

    static void set_file(FILE * file){
        if (file != nullptr){
            instance().file = file;
        }
    }

    void log(std::string str){
        if(file){
            auto t = std::chrono::high_resolution_clock::now();
            auto log = fmt::format("{:.3f} sec : {}" ,static_cast<sec_t>(t - _beg).count() , std::move(str));
            {
                std::lock_guard<std::mutex> lk(mut);
                que.push(std::move(log));
            }
            cond.notify_one();
        }
    }

    void sync_log(const std::string & str){
        if (file){
            auto t = std::chrono::high_resolution_clock::now();
            fmt::print(file , "{:.3f} sec : {}\n" , static_cast<sec_t>(t - _beg).count() , str);
            fflush(file);
        }
    }

    void end_log(){
        if(is_running){
            is_running = false;
            cond.notify_one();
        }
    }

private:
    explicit Logger()
    :is_running(true) , 
    _beg (std::chrono::high_resolution_clock::now()) ,
    t([this]{_do_print_log();}){
    
    }

    ~Logger(){
        end_log();
        if(t.joinable()) t.join();
    }

    void _do_print_log(){
        auto pred = [this]{return !que.empty() || is_running == false;};
        std::queue<std::string> log_que{};
        std::string str{};
        while(is_running){
            {
                std::unique_lock<std::mutex> lk(mut);
                cond.wait(lk , pred);
                std::swap(log_que , que);
            }

            while(!log_que.empty()){
                str = std::move(log_que.front());
                log_que.pop();
                if(file)
                    fmt::print(file , "{}\n" , std::move(str));
            }
        }
    }

private:
    std::mutex mut;
    std::condition_variable cond;
    bool is_running;

    std::chrono::time_point<std::chrono::high_resolution_clock> _beg;

    std::queue<std::string> que{};
    std::thread t;

    FILE * file{nullptr};
};

template<class tm_unit_t>
class time_elasped{

    using time_point_t = decltype(std::chrono::high_resolution_clock::now());

public:
    explicit time_elasped(uint64_t & tm_cnt)
    :_tm(tm_cnt) , _beg(std::chrono::high_resolution_clock::now())
    {}

    ~time_elasped() noexcept{
        auto _end = std::chrono::high_resolution_clock::now();
        _tm+=std::chrono::duration_cast<tm_unit_t>(_end - _beg).count();
    }

private:
    uint64_t & _tm;
    time_point_t _beg;
};
#endif