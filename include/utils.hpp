
#ifndef UTILS_INCLUDE_H
#define UTILS_INCLUDE_H

#include <chrono>
#include <type_traits>
// #include <immintrin.h>

#define UNUSED(x) (void)x

#define likely(x)      __builtin_expect(!!(x), 1)
#define unlikely(x)    __builtin_expect(!!(x), 0)

inline std::chrono::milliseconds operator "" _ms (unsigned long long ms){
    return std::chrono::milliseconds{ms};
}

struct disable_copy{
    disable_copy() = default;
    disable_copy(const disable_copy &) = delete;
    disable_copy& operator= (const disable_copy &) = delete;
};

template<class T1 , class T2>
struct ref_pair{
    T1 & first;
    T2 & second;

    explicit ref_pair(T1 & p1 , T2 & p2)
    :first(p1) , second(p2){
        
    }
};

inline bool fast_key_cmp_eq(const char * lhs , const char * rhs){
    using pcu64_t = const uint64_t *;
    return *((pcu64_t)(lhs)) == *((pcu64_t)(rhs)) && *((pcu64_t)(lhs)+1) == *((pcu64_t)(rhs)+1) ;
}

#endif