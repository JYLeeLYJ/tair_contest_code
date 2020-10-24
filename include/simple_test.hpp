#ifndef SIM_TEST_INCLUDE_H
#define SIM_TEST_INCLUDE_H

#include <fmt/format.h>
#include <string>

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
        std::cout << fmt::format("[FAIL][{}] : {}" , #func , e ) <<std::endl; \
    }\

#define ASSERT( cond ) \
    if(!(cond)) throw fmt::format("ASSERT {} " , #cond );

#endif
