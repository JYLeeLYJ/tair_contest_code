#!bin/bash

INCLUDE_DIR="../include"
LIB_PATH="../lib"
set_per_thread=$2
get_per_thread=$3

rm -rf ./judge_base
rm -rf ./judge

g++ -pthread -o judge judge.cpp random.cpp -L "../lib" -l engine -I "../include" -g -mavx2 -std=c++11

g++ -pthread -o judge_base judge.cpp random.cpp -L "../lib" -l engine_base -I "../include" -g -mavx2 -std=c++11

if [ $? -ne 0 ]; then
    echo "Compile Error"
    exit 7 
fi

echo ""
echo ""
echo "********************************"
echo "************ Bench *************"
echo "********************************"

echo ""
echo "======== [Base] =========="
# rm -f /mnt/pmem/DB
./judge_base -s $set_per_thread -g $get_per_thread

echo "======== [Engine] ========"
./judge -s $set_per_thread -g $get_per_thread

echo ""
echo ""