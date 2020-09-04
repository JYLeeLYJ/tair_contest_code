
#ifndef UTILS_INCLUDE_H
#define UTILS_INCLUDE_H

#define UNUSED(x) (void)x

#include <chrono>

std::chrono::milliseconds operator ""_ms (unsigned long long ms){
    return std::chrono::milliseconds{ms};
}

struct disable_copy{
    disable_copy() = default;
    disable_copy(const disable_copy &) = delete;
    disable_copy& operator= (const disable_copy &) = delete;
};

#endif