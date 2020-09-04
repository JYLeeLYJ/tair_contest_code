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
        std::lock_guard<std::mutex> lk(mut);
        if(file){
            que.push(std::string("[log]").append(std::move(str)));
            cond.notify_one();
        }
    }

private:
    explicit Logger():is_running(true) , t([this]{_do_print_log();}){
    }

    ~Logger(){
        is_running = false;
        cond.notify_one();
        if(t.joinable()) t.join();
    }

    void _do_print_log(){
        auto pred = [this]{return !que.empty() || is_running == false;};
        while(true){
            std::unique_lock<std::mutex> lk(mut);
            cond.wait(lk , pred);

            if (is_running == false) break;

            auto & str = que.front();
            if(file) fprintf( file , "%s\n", str.c_str());
            que.pop();
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