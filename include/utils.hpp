
#ifndef UTILS_INCLUDE_H
#define UTILS_INCLUDE_H

#include <chrono>
#include <type_traits>
#include <immintrin.h>

#define CACHELINE_SIZE 64

#define UNUSED(x) (void)x

#define likely(x)      __builtin_expect(!!(x), 1)
#define unlikely(x)    __builtin_expect(!!(x), 0)

struct disable_copy{
    disable_copy() = default;
    disable_copy(const disable_copy &) = delete;
    disable_copy& operator= (const disable_copy &) = delete;
};

struct alignas(64) atomic_uint_align64_t : std::atomic<uint32_t>{
	atomic_uint_align64_t() = default;	
	explicit atomic_uint_align64_t (uint32_t i) : std::atomic<uint32_t>(i){}

	uint32_t operator = (uint32_t i) {
		return std::atomic<uint32_t>::operator=(i);
	}
};

template<class T , std::size_t N>
struct alignas(N) align_intergral_t{
	static_assert(std::is_integral<T>{} , "T must be trivial");
	T _t{};

	align_intergral_t() = default;
	constexpr align_intergral_t(T t) : _t (t) {}

	align_intergral_t & operator = (const T & t){
		_t = t ;
		return * this;
	}

	align_intergral_t & operator++ (){
		++ _t ;
		return * this;
	}

	operator T() {
		return _t;
	}
};

static inline constexpr std::chrono::milliseconds operator "" _ms (unsigned long long ms){
    return std::chrono::milliseconds{ms};
}

static inline constexpr size_t operator "" _GB (unsigned long long n){
	return n * 1024 * 1024 * 1024 ;
}

static inline constexpr size_t operator "" _MB (unsigned long long n){
	return n * 1024 * 1024;
}

static inline constexpr size_t operator "" _KB (unsigned long long n){
	return n * 1024;
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

//https://gcc.gnu.org/bugzilla/show_bug.cgi?id=57350
inline void *align( std::size_t alignment, std::size_t size,
                void *&ptr, std::size_t &space ) {
    std::uintptr_t pn = reinterpret_cast< std::uintptr_t >( ptr );
    std::uintptr_t aligned = ( pn + alignment - 1 ) & - alignment;
    std::size_t padding = aligned - pn;
    if ( space < size + padding ) return nullptr;
    space -= padding;
    return ptr = reinterpret_cast< void * >( aligned );
}

inline std::size_t
shift_mix(std::size_t v)
{ return v ^ (v >> 47);}

static inline size_t hash_impl(uint64_t lo, uint64_t hi){
    constexpr size_t mul = (((size_t) 0xc6a4a793UL) << 32UL) + (size_t) 0x5bd1e995UL;
    constexpr size_t len = 16 , seed = 0xc70f6907UL , beg = seed ^ (len * mul);

    size_t hash = beg;
 
	const size_t data1 = shift_mix(lo * mul) * mul ;
    const size_t data2 = shift_mix(hi * mul) * mul;

	hash ^= data1;
	hash *= mul;
	hash ^= data2;
	hash *= mul;

    hash = shift_mix(hash) * mul;
    hash = shift_mix(hash);
    return hash;
}

static inline size_t hash_bytes_16(const char * ptr){	
	return hash_impl(*(uint64_t *)(ptr) , *(uint64_t *)(ptr + 8));
}

#endif