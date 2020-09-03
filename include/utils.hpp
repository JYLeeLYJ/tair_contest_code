
#ifndef UTILS_INCLUDE_H
#define UTILS_INCLUDE_H

#define UNUSED(x) (void)x

struct disable_copy{
    disable_copy() = default;
    disable_copy(const disable_copy &) = delete;
    disable_copy& operator= (const disable_copy &) = delete;
};

// struct disable_move{
//     disable_move() = default;
//     disable_move(disable_move &&) = delete;
//     disable_move& operator= (disable_move && )= delete;
// };

#endif