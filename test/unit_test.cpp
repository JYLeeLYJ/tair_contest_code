#include <iostream>
#include <string>
#include <vector>

#include "fmt/format.h"
#include "db.hpp"
#include "bloom_filter.hpp"
#include "utils.hpp"
#include "hash_index.hpp"
#include "allocator.hpp"
#include "simple_test.hpp"
#include "open_address_hash_index.hpp"
#include "lru_cache.hpp"

std::vector<std::pair<Slice , Slice>> kv_pairs{};

void test_get_set_simple() {

    auto random_str = [](unsigned int size)->char * {
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

        return str;
    };

    DB *db = nullptr;
    
    FILE * f = fopen("./performance.log" , "w");
    DB::CreateOrOpen("./DB", &db , f);
    std::unique_ptr<DB> guard(db);

    const int times = 100;

    //init keys
    for(uint i = 0 ; i < times; ++ i) {
        kv_pairs.emplace_back(Slice{random_str(16) , 16} , Slice{});
    }

    //test append new
    for(auto & kv : kv_pairs){
        Slice k = kv.first;
        uint64_t len = rand() % 1024;
        Slice v{ random_str(len) , len};
        ASSERT(db->Set(k, v) == Ok);
        std::string a;
        ASSERT(db->Get(k, &a) == Ok);
        ASSERT(a == v.to_string());
        free(v.data());
    }

    //test update 
    for(auto & kv : kv_pairs){
        Slice k = kv.first;
        uint64_t len = rand() % 1024;
        Slice v{ random_str(len) , len};
        ASSERT(db->Set(k, v) == Ok);
        std::string a;
        ASSERT(db->Get(k, &a) == Ok);
        ASSERT(a == v.to_string());
        kv.second = v;
    }

    fclose(f);
}

void test_recovery(){
    DB *db = nullptr;
    FILE * f = fopen("./performance.log" , "w");
    DB::CreateOrOpen("./DB", &db , f);
    std::unique_ptr<DB> guard(db);

    ASSERT(db);

    for(auto & kv : kv_pairs){
        std::string a{};
        ASSERT(db->Get(kv.first, &a) == Ok);
        // fmt::print("key = {}\n" , kv.first.to_string());
        // fmt::print("{}\n" , kv.second.to_string());

        ASSERT(a == kv.second.to_string());

        // free(kv.first.data());
        // free(kv.second.data());
    }

    char key_s[16];
    memset(key_s, 'a' + 20, 16);
    Slice k(key_s, 16);
    char value_s[80];
    memset(value_s, 'x', 80);
    Slice v(value_s, 80);
    ASSERT(db->Set(k, v) == Ok );

    guard.reset();

    DB::CreateOrOpen("./DB", &db , nullptr);
    guard.reset(db);

    for(auto & kv : kv_pairs){
        std::string str{};
        ASSERT(db->Get(kv.first , &str) == Ok);
        ASSERT( str == kv.second.to_string());
    }

    std::string str{};
    ASSERT(db->Get(k , &str) == Ok);
    ASSERT(str == v.to_string());
}

void test_boolean_filter(){
    bitmap_filter<34> bitset{};
    ASSERT(bitset.max_index == 40 );
    for(int i = 0 ; i< bitset.max_index ;++i){
        ASSERT(bitset.test(i) == false);
    }
    
    bitset.set(33);
    ASSERT(bitset.test(33));
    bitset.set(1);
    ASSERT(bitset.test(1));

    //undefine
    // bitset.set(40);
    // ASSERT(bitset.test(40) == false);
}

void test_fast_key_cmp(){
    std::string s1 = std::string(8 , 'a') + std::string(8 , 'b');
    std::string s2 = s1;
    std::string s3 = std::string(8 , 'b') + std::string(8 ,'b');
    std::string s4 = std::string(16, 'a');

    // fmt::print("s1 = {} , s2 = {} , s3 = {} , s4 = {}\n" , s1 , s2 ,s3 ,s4);

    ASSERT(fast_key_cmp_eq(s1.data() , s2.data()));
    ASSERT(!fast_key_cmp_eq(s1.data() , s3.data()));
    ASSERT(!fast_key_cmp_eq(s1.data() , s4.data()));
}

void test_fast_mem_cpy(){
    std::array<char , 16> key{};
    std::array<char , 80> value{};

    std::string str16(16 , 'a');
    std::string str80(80 , 'b');

    memcpy_avx_16(key.data() , str16.data());
    memcpy_avx_80(value.data() , str80.data());

    // fmt::print("key {} , value {}\n" , str16 , str80);
    // fmt::print("key {} , value {}\n" , std::string(key.data() , 16) , std::string(value.data() , 80));

    ASSERT(std::string(key.data() , 16) == str16);
    ASSERT(std::string(value.data(),80) == str80);
}

void test_hash_bytes(){
    constexpr size_t seed = 0xc70f6907UL;

    ASSERT(hash_bytes_16(std::string(16 , 'a').data()) == std::hash<std::string>{}(std::string(16,'a').data()));
}

void test_hash_index(){
    hash_index<32 , 100> hasher;
    hasher.insert(1 , 3 , 114514 , 1111);
    hasher.insert(1 , 4 , 114514 , 1112);
    
    auto bucket = hasher.get_bucket(1);
    uint cnt {0};
    for(auto & info : bucket){
        ASSERT(info.index == 114514 );
        ASSERT(info.prefix == (1111 + cnt));
        ++cnt;        
    }
}

void test_allocator(){
    value_block_allocator<6> allctr{};

    allctr.init(32,0);

    ASSERT(allctr.allocate_128() == 32);
    ASSERT(allctr.allocate_256() == 34);
    ASSERT(allctr.allocate_256() == 36);

    allctr.recollect_256(28);

    ASSERT(allctr.allocate_128() == 33);
    ASSERT(allctr.allocate_128() == 28);
    ASSERT(allctr.allocate_128() == 29);

    allctr.recollect_128(1024);
    allctr.recollect_256(34);
    
    ASSERT(allctr.allocate_128() == 1024);
    ASSERT(allctr.allocate_128() == 34);

    ASSERT(allctr.allocate_128() == 35);
    ASSERT(allctr.allocate_128() == allctr.null_index);
    ASSERT(allctr.allocate_256() == allctr.null_index);

}

void test_open_address_hash(){
    open_address_hash<32> index{};

    index.insert(1,222 , 114);
    index.insert(1,333 , 514);

    ASSERT(index.search(1,222 ,[](uint32_t key_id){return key_id == 114; }) == 114 );
    ASSERT(index.search(1,333 ,[](uint32_t key_id){return key_id == 514; }) == 514 );

    ASSERT(index.search(1,333 ,[](uint32_t key_id){return key_id == 114; })== index.null_id);
    ASSERT(index.search(1,222 ,[](uint32_t key_id){return key_id == 116; })== index.null_id);

    //more test : out of range...
}

void test_lru_cache(){
    lru_cache<int , std::string , 4> lru{};

    lru.put(1, "1111");
    ASSERT(lru.get(1) != nullptr );
    ASSERT(*lru.get(1) == "1111");

    ASSERT(lru.get(2) == nullptr);

    lru.put(2, "22222");
    lru.put(3, "33333");
    lru.put(4, "44444");

    lru.put(5,"555555");

    ASSERT(lru.get(1) == nullptr);
    ASSERT(lru.get(2));
    ASSERT(lru.get(3));
    ASSERT(lru.get(4));
    ASSERT(lru.get(5));
}

void main_get_set_unit(){
    TEST(test_get_set_simple);
    TEST(test_recovery);
}

void main_unit_test(){
    TEST(test_boolean_filter);
    // TEST(test_fast_key_cmp);
    // TEST(test_fast_mem_cpy);
    // TEST(test_hash_bytes);
    TEST(test_hash_index);
    TEST(test_allocator);
    TEST(test_open_address_hash);
    TEST(test_lru_cache);
}

int main(){
    MAIN(main_unit_test);
    MAIN(main_get_set_unit);
}