#ifndef RECORD_POOL_INCLUDE_H
#define RECORD_POOL_INCLUDE_H

#include <utility>

#include "include/db.hpp"
#include "include/memory_pool.hpp"
#include "include/utils.hpp"
#include "include/logger.hpp"

struct Record{
    static constexpr std::size_t key_size = 16 ;
    static constexpr std::size_t value_size = 80;

    char _key[key_size]{};
    char _value[value_size]{};

    Record & set (const Slice & key , const Slice& value) noexcept{
        memcpy(_key , key.data() , key.size());
        memcpy(_value , value.data() , value.size());
        return *this;
    }

    Record & update_value(const Slice & value) noexcept{
        memcpy(_value , value.data() , value.size());
        return *this;
    }

    std::string key() const {
        return std::string(_key , key_size);
    }

    std::string value() const{
        return std::string (_value , value_size);
    }
};

static_assert(sizeof(Record) == 96);

template<std::size_t N>
class record_pool{

public:
    explicit record_pool(const std::string & file_name) 
    : pool(file_name){
        Logger::instance().sync_log("record_pool init.");
    }

    Record & get_value(std::size_t index) const noexcept{
        return base()[index];
    }

    bool set_value(std::size_t index , const Slice & key , const Slice & value) noexcept{
        if(index >= N) 
            return false;

        auto & record = get_value(index);
        record.set(key , value);
        return true;
    }

    std::size_t allocate_seq() noexcept {
        auto index = seq++;
        return index >= N ? std::numeric_limits<std::size_t>::max() : index;
    }

    static inline bool is_valid_index(std::size_t i) {
        return i != std::numeric_limits<std::size_t>::max();
    }

private:
    memory_pool<N * sizeof(Record)> pool;
    std::atomic<std::size_t> seq{0};

    Record * base() const noexcept{
        return static_cast<Record *>(pool.base());
    }
};

#endif