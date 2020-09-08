
#ifndef UTILS_INCLUDE_H
#define UTILS_INCLUDE_H

#include <chrono>

#define UNUSED(x) (void)x

#define likely(x)   x 
#define unlikely(x) x

// #define likely(x)      __builtin_expect(!!(x), 1)
// #define unlikely(x)    __builtin_expect(!!(x), 0)

std::chrono::milliseconds operator "" _ms (unsigned long long ms){
    return std::chrono::milliseconds{ms};
}

struct disable_copy{
    disable_copy() = default;
    disable_copy(const disable_copy &) = delete;
    disable_copy& operator= (const disable_copy &) = delete;
};

#endif