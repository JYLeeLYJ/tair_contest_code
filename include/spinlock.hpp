#ifndef SPINLOCK_INCLUDE_H
#define SPINLOCK_INCLUDE_H

#include <atomic>

class SpinLock{
public:
    explicit SpinLock(std::atomic_flag & flag) noexcept
    :_flag(flag){
        while(_flag.test_and_set(std::memory_order_acquire));
    }

    ~SpinLock(){
        _flag.clear(std::memory_order_release);
    }

private:
    std::atomic_flag & _flag;
};

#endif