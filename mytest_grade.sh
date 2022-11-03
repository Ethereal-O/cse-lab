#!/bin/bash
for i in {0..5};
do
echo "========$i========"
    rm log/*
    ./grade.sh 2>/dev/null |grep "Passed\|passed\|failed\|Failed\|error\|Error\|score\|Score\|"
#     if [ $? -ne 0 ];
# then
#         echo "Failed mytest"
#         #exit
# fi
done
