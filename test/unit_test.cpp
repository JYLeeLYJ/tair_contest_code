#include <iostream>
#include <string>

#include "fmt/format.h"
#include "include/db.hpp"
#include "boolean_filter.hpp"

#define MAIN(func) \
    try{\
        func();\
    }catch(std::exception &e) {\
        std::cout << "\n*************\nGot exception:"<< e.what() <<std::endl; \
    }\

#define TEST(func) \
    try{\
        func();\
        std::cout<<fmt::format("[SUCC][{}]" , #func) <<std::endl; \
    }catch(std::string &e) {\
        std::cout << e <<std::endl; \
    }\

#define ASSERT( cond ) \
    if(!(cond)) throw fmt::format("[FAIL][{}] : ASSERT {} " ,__FUNCTION__ , #cond );

//********************************************************************************

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
    Slice k;
    k.size() = 16;
    Slice v;
    v.size() = 80;
    int times = 100;
    while (times--) {
        k.data() = random_str(16);
        v.data() = random_str(80);
        ASSERT(db->Set(k, v) == Ok);
        std::string a;
        ASSERT(db->Get(k, &a) == Ok);
        ASSERT(a == v.to_string());
        free(k.data());
        free(v.data());
    }
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

    bitset.set(40);
    ASSERT(bitset.test(40) == false);
}

void test_fast_key_cmp(){
    std::string s1 = std::string(8 , 'a') + std::string(8 , 'b');
    std::string s2 = s1;
    std::string s3 = std::string(8 , 'b') + std::string(8 ,'b');
    std::string s4 = std::string(16,'a');

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

void main_test_func(){
    TEST(test_get_set_simple);
    TEST(test_boolean_filter);
    TEST(test_fast_key_cmp);
    TEST(test_fast_mem_cpy);
}

int main(){
    MAIN(main_test_func);
}