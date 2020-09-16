#!bin/bash

INCLUDE_DIR="../include"
LIB_PATH="../lib"

rm -rf ./judge_base
rm -rf ./judge

g++ -pthread -o judge judge.cpp random.cpp -L $LIB_PATH -l engine -I $INCLUDE_DIR -g -mavx2 -std=c++11

# g++ -pthread -o judge_base judge.cpp random.cpp -L $LIB_PATH -l engine_base -I $INCLUDE_DIR -g -mavx2 -std=c++11

if [ $? -ne 0 ]; then
    echo "Compile Error"
    exit 7 
fi

echo ""
echo ""
echo "********************************"
echo "************ Test **************"
echo "********************************"

./judge

echo ""
echo ""