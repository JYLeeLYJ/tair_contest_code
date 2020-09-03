#include <mutex>
#include <condition_variable>

struct disable_copy{
    disable_copy() = default;
    disable_copy(const disable_copy &) = delete;
    disable_copy& operator= (const disable_copy &) = delete;
};

struct disable_move{
    disable_move() = default;
    disable_move(disable_move &&) = delete;
    disable_move& operator= (disable_move && )= delete;
};

class RW_Lock : disable_copy , disable_move{

    using guard = std::unique_lock<std::mutex>;

public:
    explicit RW_Lock() = default;

    void lock_read(){
        std::unique_lock<std::mutex> lk(mut);
        read_cond.wait(lk , [this]{
            return write_cnt == 0;
        });
        ++read_cnt;
    }
    
    void lock_write(){
        std::unique_lock<std::mutex> lk(mut);
        ++write_cnt;
        write_cond.wait(lk ,[this]{
            return read_cnt == 0 && is_write == false;
        });
        is_write = true;
    }
    
    void release_read(){
        std::unique_lock<std::mutex> lk(mut);
        if(--read_cnt == 0) write_cond.notify_one();
    }
    
    void release_write(){
        std::unique_lock<std::mutex> lk(mut);
        is_write == false;
        if (--write_cnt == 0) 
            read_cond.notify_all();
        else 
            write_cond.notify_one();

    }

private:
    int read_cnt{0} ;
    int write_cnt{0} ;
    bool is_write{false};

    std::mutex mut{};
    std::condition_variable read_cond{};
    std::condition_variable write_cond{};
};


class Read_Guard : disable_copy , disable_move{
public:
    explicit Read_Guard(RW_Lock & l):lock(l){
        lock.lock_read();
    }

    ~Read_Guard(){
        lock.release_read();
    }

private:
    RW_Lock & lock;
};

class Wrtie_Guard : disable_copy , disable_move{
public:
    explicit Wrtie_Guard(RW_Lock &l):lock(l){
        lock.lock_write();
    }
    ~Wrtie_Guard(){
        lock.release_write();
    }

private:
    RW_Lock & lock;
};