#!/bin/bash
USER=vothanh
HDFS_DIR=/user/$USER
HDFS_INPUT_DIR=$HDFS_DIR/question6/input
HDFS_OUTPUT_DIR=$HDFS_DIR/question6/output

# Upload file to HDFS
hdfs dfs -mkdir -p $HDFS_INPUT_DIR/test1
hdfs dfs -mkdir -p $HDFS_INPUT_DIR/test2
hdfs dfs -mkdir -p $HDFS_INPUT_DIR/test3
hdfs dfs -mkdir -p $HDFS_OUTPUT_DIR
hdfs dfs -put test/test1/input/* $HDFS_INPUT_DIR/test1
hdfs dfs -put test/test2/input/* $HDFS_INPUT_DIR/test2
hdfs dfs -put test/test3/input/* $HDFS_INPUT_DIR/test3