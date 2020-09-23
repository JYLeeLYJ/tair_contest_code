
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

inline void fast_align_mem_cpy_16(char * dst , const char * src){
    *(uint64_t *)(dst) = *(uint64_t *)(src);    //1   
    *(uint64_t *)(dst+8) = *(uint64_t *)(src+8);    //2
}

inline void fast_align_mem_cpy_80(char * dst , const char * src){
    *(uint64_t *)(dst) = *(uint64_t *)(src);    //1   
    *(uint64_t *)(dst+8) = *(uint64_t *)(src+8);    //2
    *(uint64_t *)(dst+16) = *(uint64_t *)(src+16);    //3
    *(uint64_t *)(dst+24) = *(uint64_t *)(src+24);    //4
    *(uint64_t *)(dst+32) = *(uint64_t *)(src+32);    //5
    *(uint64_t *)(dst+40) = *(uint64_t *)(src+40);    
    *(uint64_t *)(dst+48) = *(uint64_t *)(src+48);    //7
    *(uint64_t *)(dst+56) = *(uint64_t *)(src+56);    
    *(uint64_t *)(dst+64) = *(uint64_t *)(src+64);    //9
    *(uint64_t *)(dst+72) = *(uint64_t *)(src+72);    
}

#endif