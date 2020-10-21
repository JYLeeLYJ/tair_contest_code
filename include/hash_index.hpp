#ifndef HASH_INDEX_INCLUDE_H
#define HASH_INDEX_INCLUDE_H

#include <array>
#include <atomic>
#include <memory>

template<std::size_t n_bucket , std::size_t n_pool>
class hash_index{
public:

    static constexpr std::size_t bucket_cnt = n_bucket;
    static constexpr uint32_t null_id = 0xffffffff;

    struct index_info{
        uint32_t prefix;
        uint32_t index;
    };

    struct index_info_node : index_info{
        uint32_t next_info_id;
    };

    //for range-for usage
    struct bucket_iterator{
        std::array<index_info_node , n_pool> & pool;
        uint32_t id;

        explicit bucket_iterator(std::array<index_info_node , n_pool> & pool_ , uint32_t id) 
        : pool(pool_), id(id) {}

        bucket_iterator & operator ++(){
            id = (id != hash_index::null_id ) ? pool[id].next_info_id : id;
            return *this;
        }

        //warning : not assert pool == pool
        bool operator == (const bucket_iterator & it) const{
            return id == it.id;
        }

        bool operator != (const bucket_iterator & it) const {
            return ! operator==(it);
        }

        //warning : undefine if out of range
        index_info & operator * () const{
            return static_cast<index_info &>(pool[id]);
        }
    };

    struct bucket{
        bucket_iterator beg;

        explicit bucket(std::array<index_info_node , n_pool> & pool_ , uint32_t id)
        : beg(pool_ , id){}

        bucket_iterator begin() const {
            return beg;
        }

        bucket_iterator end() const{
            return bucket_iterator{beg.pool , null_id};
        }
    };

public:

    explicit hash_index() noexcept{
        auto bks = new uint32_t[n_bucket];

        memset(index_pool.data() , 0xff, sizeof(index_info) * n_pool);
        memset(bks , 0xff , sizeof(uint32_t) * n_bucket);

        index_bucket.reset(reinterpret_cast<std::atomic<uint32_t> *>(bks));
    }

    void insert(uint32_t i_bucket , uint32_t index_info_id , uint32_t key_index , uint32_t prefix ){
        index_pool[index_info_id].index = key_index;
        index_pool[index_info_id].prefix = prefix;
        index_pool[index_info_id].next_info_id = null_id;

        auto bk = i_bucket;
        auto id = index_bucket[bk].load();
        if( id == null_id 
        && index_bucket[bk].compare_exchange_weak(id , index_info_id , std::memory_order_release , std::memory_order_relaxed)){
            return ;
        }

        while(id != null_id){
            uint32_t next = index_pool[id].next_info_id;
            if(next == null_id 
            && reinterpret_cast<std::atomic<uint32_t>&>(index_pool[id].next_info_id).compare_exchange_weak(
                next , index_info_id , std::memory_order_release , std::memory_order_relaxed
            )){
                return ;
            }
            id = next;
        }
    }

    bucket get_bucket(uint32_t i){
        return bucket{index_pool , index_bucket[i]};
    }

private:
    std::array<index_info_node , n_pool> index_pool;
    std::unique_ptr<std::atomic<uint32_t>[]> index_bucket;
};

#endif