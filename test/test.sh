rm -rf ./test

g++ -std=c++11 -o test -g -I.. test.cpp -L../lib -lengine -lpthread -lrt -lz #-lpmem

# rm -rf ./tmp

if [ ! -f "tmp" ]; then
    g++ init.cpp -o init_tmp_file
    ./init_tmp_file
fi

echo ""
echo "********** TEST *************"

./test

echo "********** FIN **************"
echo 