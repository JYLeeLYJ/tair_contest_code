#include <iostream>
#include <string>
#include "fmt/format.h"

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

void main_test_func(){
    TEST(test_boolean_filter);
    TEST(test_fast_key_cmp);
}

int main(){
    MAIN(main_test_func);
}