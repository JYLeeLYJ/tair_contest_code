#ifndef TAIR_CONTEST_INCLUDE_DB_H_
#define TAIR_CONTEST_INCLUDE_DB_H_

#include <cstdio>
#include <string>

enum Status : unsigned char {
    Ok,
    NotFound,
    IOError,
    OutOfMemory

};

struct Slice {
    char* data;
    uint64_t size;
};

class DB {
public:
    /*
     *  Recover or create db from pmem-file.
     *  It's not required to implement the recovery in round 1.
     *  You can assume that the file does not exist.
     *  You should write your log to the log_file. 
     *  Stdout, stderr would be redirect to /dev/null.
     */
    static Status Recover(const std::string& name, DB** dbptr, FILE* log_file = nullptr);

    /*
     *  Get the value of key.
     *  If the key does not exist the KeyNotFound is returned.
     */
    virtual Status Get(const Slice& key, std::string* value) = 0;

    /*
     *  Set key to hold the string value.
     *  If key already holds a value, it is overwritten. 
     */
    virtual Status Set(const Slice& key, const Slice& value) = 0;

    /*
     * Close the db on exit.
     */
    virtual ~DB() = 0;
};

#endif