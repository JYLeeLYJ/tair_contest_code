#!bin/bash

INCLUDE_DIR="../include"
LIB_PATH="../lib"

rm -rf ./judge
rm -rf ./DB

g++ -pthread -o judge judge.cpp random.cpp -L $LIB_PATH -lengine -lpmem -I $INCLUDE_DIR -g -mavx2 -std=c++11 -O2

if [ $? -ne 0 ]; then
    echo "Compile Error"
    exit 7 
fi

echo ""
echo ""
echo "********************************"
echo "******* Correctness Test *******"
echo "********************************"

./judge

echo ""
echo ""