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

void main_test_func(){
    TEST(test_boolean_filter);
}

int main(){
    MAIN(main_test_func);
}