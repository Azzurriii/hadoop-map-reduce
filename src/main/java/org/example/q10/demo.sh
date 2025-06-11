#!/bin/bash

# Question 10 demo script
# Usage: ./demo.sh [test1|test2|test3]

USER=vothanh
HDFS_DIR=/user/$USER
HDFS_INPUT_DIR=$HDFS_DIR/question10/input
HDFS_OUTPUT_DIR=$HDFS_DIR/question10/output

if [ $# -eq 0 ]; then
    echo "Usage: $0 [test1|test2|test3]"
    exit 1
fi

TEST_CASE=$1

case $TEST_CASE in
    "test1")
        echo "Running test1"
        hadoop jar question10.jar Question10 $HDFS_INPUT_DIR/test1 $HDFS_OUTPUT_DIR/test1
        echo "Results:"
        hdfs dfs -cat $HDFS_OUTPUT_DIR/test1/part-r-00000
        ;;
    "test2")
        echo "Running test2"
        hadoop jar question10.jar Question10 $HDFS_INPUT_DIR/test2 $HDFS_OUTPUT_DIR/test2
        echo "Results:"
        hdfs dfs -cat $HDFS_OUTPUT_DIR/test2/part-r-00000
        ;;
    "test3")
        echo "Running test3"
        hadoop jar question10.jar Question10 $HDFS_INPUT_DIR/test3 $HDFS_OUTPUT_DIR/test3
        echo "Results:"
        hdfs dfs -cat $HDFS_OUTPUT_DIR/test3/part-r-00000
        ;;
    *)
        echo "Invalid test case. Use test1, test2, or test3"
        exit 1
        ;;
esac