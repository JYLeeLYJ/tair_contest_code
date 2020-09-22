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
#include <immintrin.h>
#include <chrono>
#include "random.h"

#include "db.hpp"

using namespace std;

typedef unsigned long long ull;

const int NUM_THREADS = 16;
int PER_SET = 4800;
int PER_GET = 4800;
const ull BASE = 199997;
struct  timeval TIME_START, TIME_END;
const int MAX_POOL_SIZE = 1e5;
std::atomic<int> POOL_TOP {0};
ull key_pool[MAX_POOL_SIZE];
ull value_pool[MAX_POOL_SIZE * 5];
int MODE = 1;

DB* db = nullptr;

vector<uint16_t> pool_seed[16];

ull seed[] = {
    19, 31, 277, 131, 97, 2333, 19997, 22221, 
    217, 89, 73, 31, 17,
    255, 103, 207
};

bool is_correct{true};

void init_pool_seed() {
    for(int i = 0; i < 16; i++) {
        pool_seed[i].resize(16);
        pool_seed[i][0] = seed[i];
        for(int j = 1; j < 16; j++) {
            pool_seed[i][j] = pool_seed[i][j-1] * pool_seed[i][j-1];
        }
    }
}

void* set_pure(void * id) {

    ull thread_id = (ull*)id - seed;
    Random rnd;
    std::string value{};

    int cnt = PER_SET;
    while(cnt -- ) {

        unsigned int* start = rnd.nextUnsignedInt();

        Slice data_key((char*)start, 16);
        Slice data_value((char*)(start + 4), 80);

        if((cnt & 0x0003)== 0) {
            auto off = POOL_TOP.fetch_add(2);
            memcpy(key_pool + off, start, 16); 
            // memcpy(value_pool + off * 5 , start , 80);          
            // POOL_TOP += 2;
        }
	    if(db->Set(data_key, data_value)!=Ok){
            is_correct = false;
            perror("Set Correctness Failed.");
            exit(0);
        }
        if(db->Get(data_key,&value) != Ok){
            is_correct = false;
            perror("get status incorrect.");
            exit(0);
        }
        if(value != data_value.to_string()){
            is_correct = false;
            perror("get value incorrect.");
            exit(0);
        }
    }
    return 0;
}

void* get_pure(void *id) {
    int thread_id = (ull*)id - seed;

    Random rnd;

    mt19937 mt(23333);
    double u = POOL_TOP / 2.0;
    double o = POOL_TOP * 0.01;
    int edge = POOL_TOP * 0.0196;
    normal_distribution<double> n(u, o);
    string value = "";

    int cnt = PER_GET;
    while(cnt --) {
        int id = ((int)n(mt) | 1) ^ 1;
        id %= POOL_TOP - 2;
        Slice data_key((char*)(key_pool + id), 16);
        if(db->Get(data_key, &value) != Ok){
            is_correct = false;
            // perror("Get Correctness Failed.");
            // exit(0);
        }
    }
    return 0;
}

void test_set_pure(pthread_t * tids) {
    for(int i = 0; i < NUM_THREADS; ++i) {
        int ret = pthread_create(&tids[i], NULL, set_pure, seed+i);
        if(ret != 0) {
            printf("create thread failed.\n");
            exit(1);
        }
    }

    for(int i = 0; i < NUM_THREADS; i++){
        pthread_join(tids[i], NULL);
    }

}

void test_set_get(pthread_t * tids) {
    for(int i = 0; i < NUM_THREADS; ++i) {
        int ret = pthread_create(&tids[i], NULL, get_pure, seed+i);
    }

    for(int i = 0; i < NUM_THREADS; i++){
        pthread_join(tids[i], NULL);
    }
}

int main(int argc, char *argv[]) {

    init_pool_seed();

    FILE * log_file =  fopen("./performance.log", "w");

    DB::CreateOrOpen("./DB", &db, log_file);

    pthread_t tids[NUM_THREADS];

    auto t1 = std::chrono::high_resolution_clock::now();
    test_set_pure(tids);
    auto t2 = std::chrono::high_resolution_clock::now();
    test_set_get(tids);
    auto t3 = std::chrono::high_resolution_clock::now();

    using namespace std::chrono;
    using ms_t = duration<float , std::milli>;

    printf("time set:%.2lf\ntime set/get:%.2lf\n", ms_t(t2 - t1).count(), ms_t(t3-t2).count());
    if(!is_correct) printf("[Correctness Failed.]");
    return 0;
}
