#!/usr/bin/env bash

if [ $# -ne 1 ]; then
    echo "Usage: $0 numTrials"
    exit 1
fi

trap 'kill -INT -$pid; exit 1' INT

runs=$1

for i in $(seq 1 $runs); do
    echo '-----------------------------------'
    echo '***' RUNNING TESTING TRIAL $i '***'

    timeout -k 2s 1000s go test -race -run '3A|3B'

    if [ $? -ne 0 ]; then
        echo '***' FAILED TESTS IN TRIAL $i
        exit 1
    fi

    echo '***' PASSED TESTING TRIAL $i '***'
done
echo '***' PASSED ALL $i TESTING TRIALS '***'
