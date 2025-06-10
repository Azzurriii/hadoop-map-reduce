#!/bin/bash

# Script chạy MapReduce Question10 với test case cụ thể
# Sử dụng: ./run.sh <test_case>
# Ví dụ: ./run.sh test1
#        ./run.sh test2
#        ./run.sh test3

if [ $# -lt 1 ]; then
    echo "Sử dụng: $0 <test_case>"
    echo "Ví dụ: $0 test1"
    echo "       $0 test2"
    echo "       $0 test3"
    echo ""
    echo "Test cases có sẵn:"
    ls -d test/test* 2>/dev/null | sed 's|test/||' || echo "Không tìm thấy test case nào"
    exit 1
fi

TEST_CASE=$1
INPUT_DIR="/user/input/question10_${TEST_CASE}"
OUTPUT_DIR="/user/output/question10_${TEST_CASE}"

# Kiểm tra test case có tồn tại không
if [ ! -d "test/${TEST_CASE}" ]; then
    echo "Lỗi: Test case '${TEST_CASE}' không tồn tại!"
    echo "Test cases có sẵn:"
    ls -d test/test* 2>/dev/null | sed 's|test/||' || echo "Không tìm thấy test case nào"
    exit 1
fi

echo "=== Chạy RIGHT OUTER JOIN cho ${TEST_CASE} ==="

echo "Bước 1: Chuẩn bị dữ liệu cho ${TEST_CASE}"
hdfs dfs -mkdir -p $INPUT_DIR
hdfs dfs -rm -r $INPUT_DIR/* 2>/dev/null || true
hdfs dfs -put test/${TEST_CASE}/input/* $INPUT_DIR/

echo "Bước 2: Xóa thư mục output cũ"
hdfs dfs -rm -r $OUTPUT_DIR 2>/dev/null || true

echo "Bước 3: Chạy MapReduce Job"
hadoop jar question10.jar Question10 $INPUT_DIR $OUTPUT_DIR

if [ $? -eq 0 ]; then
    echo ""
    echo "=== Kết quả RIGHT OUTER JOIN cho ${TEST_CASE} ==="
    hdfs dfs -cat $OUTPUT_DIR/part-r-00000
    echo ""
    echo "Job hoàn thành thành công!"
    
    echo ""
    echo "=== Thông tin dữ liệu input ==="
    echo "Dữ liệu ${TEST_CASE}:"
    hdfs dfs -cat $INPUT_DIR/*
else
    echo "Job thất bại!"
    exit 1
fi