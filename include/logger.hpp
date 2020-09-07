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

class Logger:disable_copy{

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
            {
                std::lock_guard<std::mutex> lk(mut);
                que.push(std::move(str));
            }
            cond.notify_one();
        }
    }

    void sync_log(const std::string & str){

        static auto beg = std::chrono::high_resolution_clock::now();

        if (file){
            auto t = std::chrono::high_resolution_clock::now();
            using min_t = std::chrono::duration<float , std::ratio<60>>;
            fprintf(file , "%.2f min : %s\n" , static_cast<min_t>(t - beg).count() ,str.data());
            fflush(file);
        }
    }

    void end_log(){
        is_running = false;
        cond.notify_one();
    }

private:
    explicit Logger()
    :is_running(true) , t([this]{_do_print_log();})
    {}

    ~Logger(){
        end_log();
        if(t.joinable()) t.join();
    }

    void _do_print_log(){
        auto pred = [this]{return !que.empty() || is_running == false;};
        std::queue<std::string> log_que{};
        std::string str{};
        while(true){
            {
                std::unique_lock<std::mutex> lk(mut);
                cond.wait(lk , pred);
                std::swap(log_que , que);
            }

            while(!log_que.empty()){
                str = std::move(log_que.front());
                log_que.pop();
                if(file) fprintf( file , "%s\n", str.c_str());
            }

            if (is_running == false) break;

        }
    }

private:
    std::mutex mut;
    std::condition_variable cond;
    bool is_running;

    std::queue<std::string> que{};
    std::thread t;

    FILE * file{nullptr};
};

#endif