#!/bin/bash
for i in {0..5};
do
echo "============== $i =============="
    rm -rf raft_temp
    ./grade.sh 2>./mytest_lab3_err.log 1>./mytest_lab3.log
    cat ./mytest_lab3.log | grep "Fail" || echo "Pass all" 
    cat ./mytest_lab3_err.log | grep -v "rpcs::dispatch: unknown proc"
    cat ./mytest_lab3.log | grep "Final score:"
    rm ./mytest_lab3.log ./mytest_lab3_err.log
done