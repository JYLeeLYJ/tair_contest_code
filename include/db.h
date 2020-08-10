#ifndef TAIR_CONTEST_INCLUDE_DB_H_
#define TAIR_CONTEST_INCLUDE_DB_H_

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
     */
    static Status Recover(const std::string& name, DB** dbptr);

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