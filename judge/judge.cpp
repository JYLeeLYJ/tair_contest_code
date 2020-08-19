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
#include "random.h"

#include "db.hpp"

using namespace std;

typedef unsigned long long ull;

const int NUM_THREADS = 16;
int PER_SET = 48000000;
int PER_GET = 48000000;
const ull BASE = 199997;
struct  timeval TIME_START, TIME_END;
const int MAX_POOL_SIZE = 5e7;
atomic<int> POOL_TOP(0);
ull key_pool[MAX_POOL_SIZE];
int MODE = 1;
int TOTAL_KEY_SIZE = 5e7;

DB* db = nullptr;

vector<uint16_t> pool_seed[16];

ull seed[] = {
    133, 117, 23, 59, 1997, 31, 83, 13, 
    217, 89, 107, 391, 917,
    255, 1024, 207
};


void init_pool_seed() {
    for(int i = 0; i < 16; i++) {
        pool_seed[i].resize(16);
        pool_seed[i][0] = seed[i];
        for(int j = 1; j < 16; j++) {
            pool_seed[i][j] = pool_seed[i][j-1] * pool_seed[i][j-1];
        }
    }
}

void* set_pure(void *id) {

    int thread_id = (ull*)id - seed;
    Random rnd(pool_seed[thread_id]);

    // 记录千分之一的key
    int cnt = PER_SET;

    while(cnt -- ) {
        unsigned int* start = rnd.nextUnsignedInt();
        Slice data_key((char*)start, 16);
        Slice data_value((char*)(start + 4), 80);
        
        if(((cnt & 0x7777) ^ 0x7777) == 0) {
            memcpy(key_pool + POOL_TOP, start, 16);           
            POOL_TOP += 2;
        }
	    db->Set(data_key, data_value);
    }
    return 0;
}

void* get_pure(void *id) {
    int thread_id = (ull*)id - seed;

    Random rnd(pool_seed[thread_id]);

    mt19937 mt(23333);
    normal_distribution<double> n(POOL_TOP / 2, 5000);
    string value = "";

    int cnt = PER_GET;
    while(cnt --) {
        int id = ((int)n(mt) | 1) ^ 1;
        if( id - POOL_TOP/2 > 8000) {
            // 写
            id %= POOL_TOP;
            unsigned int* start = rnd.nextUnsignedInt();
            Slice data_key((char*)(key_pool + id), 16);
            Slice data_value((char*)start, 80);
	        (*db).Set(data_key, data_value);
        } else {
            // 读
            Slice data_key((char*)(key_pool + id), 16);
            (*db).Get(data_key, &value);
        }
    }
    return 0;
}


void config_parse(int argc, char *argv[]) {

    int opt = 0;

    while((opt = getopt(argc, argv, "hkm:s:g:")) != -1) {
        switch(opt) {
            case 'h':
                printf("Usage: ./judge -m <judge-mode> -s <set-size-per-Thread> -g <get-size-per-Thread>\n");
                return ;
            case 'm':
                MODE = atoi(optarg);
                break;
            case 's':
                PER_SET = atoi(optarg);
                break;
            case 'g':
                PER_GET = atoi(optarg);
                break;
            case 'k':
                TOTAL_KEY_SIZE = atoi(optarg);
                break;
        }
    }
}

void test_set_pure(pthread_t * tids) {
    // 16线程纯写入
    for(int i = 0; i < NUM_THREADS; ++i) {
        //参数依次是：创建的线程id，线程参数，调用的函数，传入的函数参数
        int ret = pthread_create(&tids[i], NULL, set_pure, seed+i);
    }

    // 阻塞到线程全部结束
    for(int i = 0; i < NUM_THREADS; i++){
        pthread_join(tids[i], NULL);
    }

}


void test_set_get(pthread_t * tids) {
    // 只读存在的Key
    for(int i = 0; i < NUM_THREADS; ++i) {
        //参数依次是：创建的线程id，线程参数，调用的函数，传入的函数参数
        int ret = pthread_create(&tids[i], NULL, get_pure, seed+i);
    }

    // 阻塞到线程全部结束
    for(int i = 0; i < NUM_THREADS; i++){
        pthread_join(tids[i], NULL);
    }
}



int main(int argc, char *argv[]) {

    config_parse(argc, argv);

    init_pool_seed();

    gettimeofday(&TIME_START,NULL);

    FILE * log_file =  fopen("../tasks/performance.log", "w");

    DB::CreateOrOpen("/mnt/pmem/DB", &db, log_file);

    pthread_t tids[NUM_THREADS];

    test_set_pure(tids);
    gettimeofday(&TIME_END,NULL);

    ull sec_set = 1000000 * (TIME_END.tv_sec-TIME_START.tv_sec)+ (TIME_END.tv_usec-TIME_START.tv_usec);
    // test_set_get(tids);
    gettimeofday(&TIME_END,NULL);
    ull sec_total = 1000000 * (TIME_END.tv_sec-TIME_START.tv_sec)+ (TIME_END.tv_usec-TIME_START.tv_usec);
    ull sec_set_get = sec_total - sec_set;

    printf("%.2lf\n%.2lf\n", sec_set/1000.0, sec_set_get/1000.0);

    return 0;
}
