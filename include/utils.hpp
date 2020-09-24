
#ifndef UTILS_INCLUDE_H
#define UTILS_INCLUDE_H

#include <chrono>
#include <type_traits>
#include <immintrin.h>

#define UNUSED(x) (void)x

#define likely(x)      __builtin_expect(!!(x), 1)
#define unlikely(x)    __builtin_expect(!!(x), 0)

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

struct alignas(64) atomic_uint_align64_t : std::atomic<uint32_t>{
	atomic_uint_align64_t() = default;	
	explicit atomic_uint_align64_t (uint32_t i) : std::atomic<uint32_t>(i){}

	uint32_t operator = (uint32_t i) {
		return std::atomic<uint32_t>::operator=(i);
	}
};

inline std::chrono::milliseconds operator "" _ms (unsigned long long ms){
    return std::chrono::milliseconds{ms};
}

static inline bool fast_key_cmp_eq(const char * lhs , const char * rhs){
    using pcu64_t = const uint64_t *;
    return *((pcu64_t)(lhs)) == *((pcu64_t)(rhs)) && *((pcu64_t)(lhs)+1) == *((pcu64_t)(rhs)+1) ;
}

static inline void memcpy_avx_16(void *dst, const void *src) {
#if 1
	__m128i m0 = _mm_loadu_si128(((const __m128i*)src) + 0);
	_mm_storeu_si128(((__m128i*)dst) + 0, m0);
#else
	*((uint64_t*)((char*)dst + 0)) = *((uint64_t*)((const char*)src + 0));
	*((uint64_t*)((char*)dst + 8)) = *((uint64_t*)((const char*)src + 8));
#endif
}

static inline void memcpy_avx_32(void *dst, const void *src) {
	__m256i m0 = _mm256_loadu_si256(((const __m256i*)src) + 0);
	_mm256_storeu_si256(((__m256i*)dst) + 0, m0);
}

static inline void memcpy_avx_64(void *dst, const void *src) {
	__m256i m0 = _mm256_loadu_si256(((const __m256i*)src) + 0);
	__m256i m1 = _mm256_loadu_si256(((const __m256i*)src) + 1);
	_mm256_storeu_si256(((__m256i*)dst) + 0, m0);
	_mm256_storeu_si256(((__m256i*)dst) + 1, m1);
}

static inline void memcpy_avx_80(void * dst , const void * src){
    memcpy_avx_16(dst , src);
    memcpy_avx_64((char*)dst + 16 , (char*)src + 16);
}

static inline void memcpy_avx_128(void *dst, const void *src) {
	__m256i m0 = _mm256_loadu_si256(((const __m256i*)src) + 0);
	__m256i m1 = _mm256_loadu_si256(((const __m256i*)src) + 1);
	__m256i m2 = _mm256_loadu_si256(((const __m256i*)src) + 2);
	__m256i m3 = _mm256_loadu_si256(((const __m256i*)src) + 3);
	_mm256_storeu_si256(((__m256i*)dst) + 0, m0);
	_mm256_storeu_si256(((__m256i*)dst) + 1, m1);
	_mm256_storeu_si256(((__m256i*)dst) + 2, m2);
	_mm256_storeu_si256(((__m256i*)dst) + 3, m3);
}
#endif