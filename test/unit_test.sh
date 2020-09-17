#!bin/bash

EXTERNEL_DIR="../external"
INCLUDE_DIR="../include"
LIB_PATH="../lib"

rm -rf ./unit_test

g++ unit_test.cpp -o unit_test -L$LIB_PATH -I$INCLUDE_DIR -I$EXTERNEL_DIR -I.. -DFMT_HEADER_ONLY -g -std=c++11

if [ $? -ne 0 ]; then
    echo "Compile Error"
    exit 7 
fi

echo ""
echo ""
echo "********************************"
echo "********** Unit Test ***********"
echo "********************************"

./unit_test

echo ""
echo ""