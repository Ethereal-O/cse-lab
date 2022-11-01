for i in {0..5};
do
echo "========$i========"
    ./test-lab2a-part2.sh 2>/dev/null |grep "Passed B" | awk 'END {print}'
#     if [ $? -ne 0 ];
# then
#         echo "Failed mytest"
#         #exit
# fi
done