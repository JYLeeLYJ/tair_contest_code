#include <stdio.h>
#include <iostream>
#include <time.h>
#include <pthread.h>
#include <sys/time.h>
#include <string.h>
#include <getopt.h>
#include <unistd.h>
#include <stdlib.h>
#include <string>
#include <random>
#include <atomic> 
#include <array>
#include <thread>
#include <immintrin.h>
#include <chrono>
#include "random.h"

#include "db.hpp"

using namespace std;

typedef unsigned long long ull;

const int NUM_THREADS = 16;
uint PER_SET = 2400;
uint PER_GET = PER_SET;

DB* db = nullptr;
std::atomic<uint> thread_seq{0};

bool is_correct {true};

std::vector<std::pair<Slice, Slice>> kv_pool;

Slice random_str(unsigned int size){
    char *str = (char *)malloc(size + 1);
    for (unsigned int i = 0; i < size; i++) {
        switch (rand() % 3) {
        case 0:
            str[i] = rand() % 10 + '0';
            break;
        case 1:
            str[i] = rand() % 26 + 'A';
            break;
        case 2:
            str[i] = rand() % 26 + 'a';
            break;
        default:
            break;
        }
    }
    str[size] = 0;
    return {str , size};
};

void init_pool(){
    for(uint i = 0 ; i < PER_SET * NUM_THREADS ; ++ i){
        kv_pool.emplace_back(random_str(16) , random_str(rand() % 128 + 80));
    }
}

void set_pure() {
    uint bk = (thread_seq ++ ) % NUM_THREADS;
    for(uint i = 0 ; i < PER_SET ; ++i){
        const auto & kv = kv_pool[ bk * PER_SET + i];
        auto sta = db->Set(kv.first , kv.second);
        if(sta != Status::Ok){
            is_correct = false;
            // printf("set failed.\n");
            return;
        }
        std::string str{};
        if(db->Get(kv.first ,&str) != Ok ){
            is_correct = false;
            // printf("No.%d get after set failed.\n" , i);
            return ;
        }

        if(str != kv.second.to_string()){
            is_correct = false;
            // printf("No.%d get after set incorrect.\n" , i);
            return ;
        }
    }
}

void get_pure() {
    uint bk = (thread_seq ++ ) % NUM_THREADS;
    for(uint i = 0 ; i < PER_GET ; ++i ){
        uint index = bk * PER_SET + i;
        auto & kv = kv_pool[ index ];
        std::string v{};
        auto sta = db ->Get(kv.first , &v);
        if(sta != Ok){
            is_correct = false;
            // printf("not found\n");
            return ;
        }
        if(v != kv.second.to_string()){
            is_correct = false;
            // printf("incorrect value.\n");
            return ;
        }
    }
}

void test_set_pure() {
    std::array<std::thread , NUM_THREADS> tids;
    for(auto & t : tids){
        t = std::thread(set_pure);
    }

    for(auto & t : tids){
        t.join();
    }
}

void test_set_get() {
    std::array<std::thread , NUM_THREADS> tids;
    for(auto & t : tids){
        t = std::thread(get_pure);
    }

    for(auto & t : tids){
        t.join();
    }

}

int main(int argc, char *argv[]) {

    init_pool();

    FILE * log_file =  fopen("./performance.log", "w");

    DB::CreateOrOpen("./DB", &db, log_file);

    auto t1 = std::chrono::high_resolution_clock::now();
    test_set_pure();
    auto t2 = std::chrono::high_resolution_clock::now();

    if(!is_correct){
        printf("[Correctness Failed.]");
        return 0;
    }

    auto t3 = std::chrono::high_resolution_clock::now();
    test_set_get();
    auto t4 = std::chrono::high_resolution_clock::now();

    using namespace std::chrono;
    using ms_t = duration<float , std::milli>;
    if(!is_correct) printf("[Correctness Failed.]");
    else printf("time set:%.2lf\ntime set/get:%.2lf\n", ms_t(t2 - t1).count(), ms_t(t4-t3).count());
    return 0;
}
