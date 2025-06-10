#!/bin/bash

QUERY_POINT=$1
INPUT_DIR="/user/input/question6"
OUTPUT_DIR="/user/output/question6"

echo "Bước 1: Compile chương trình Java"
mkdir -p classes
javac -classpath $HADOOP_HOME/share/hadoop/common/*:$HADOOP_HOME/share/hadoop/mapreduce/*:$HADOOP_HOME/share/hadoop/hdfs/*:$HADOOP_HOME/share/hadoop/common/lib/* -d classes Question7.java

if [ $? -ne 0 ]; then
    echo "Lỗi compile. Kiểm tra lại code."
    exit 1
fi

echo "Bước 2: Tạo file JAR" 
jar cf question7.jar -C classes .

echo "Bước 3: Chuẩn bị dữ liệu trên HDFS"
hdfs dfs -mkdir -p $INPUT_DIR
hdfs dfs -put src/main/java/org/example/q6/test/test*/input/* $INPUT_DIR/ 2>/dev/null || echo "File đã tồn tại trên HDFS"

echo "Bước 4: Xóa thư mục output cũ"
hdfs dfs -rm -r $OUTPUT_DIR 2>/dev/null || true