#!/bin/bash

# Script để chạy các demo Spark Streaming
# Tác giả: Nhóm Big Data Applications

echo "🎯 Spark Streaming Demo Runner"
echo "=============================="

# Kiểm tra Python và PySpark
if ! python3 -c "import pyspark" &> /dev/null; then
    echo "❌ PySpark chưa được cài đặt!"
    echo "📦 Cài đặt: pip install pyspark"
    exit 1
fi

# Menu lựa chọn demo
echo "📋 Chọn demo để chạy:"
echo "1. Word Count cơ bản (word_count_streaming.py)"
echo "2. Word Count với filter >4 ký tự (word_count_filtered.py)"  
echo "3. Demo nâng cao với windowing (advanced_streaming.py)"
echo "4. Thoát"

read -p "🔢 Nhập lựa chọn (1-4): " choice

case $choice in
    1)
        echo "🚀 Chạy Word Count cơ bản..."
        echo "💡 Nhớ khởi động netcat: nc -lk 9999"
        sleep 2
        python3 src/word_count_streaming.py
        ;;
    2)
        echo "🎯 Chạy Word Count với filter..."
        echo "💡 Nhớ khởi động netcat: nc -lk 9999"
        sleep 2
        python3 src/word_count_filtered.py
        ;;
    3)
        echo "⚡ Chạy Demo nâng cao với windowing..."
        echo "💡 Nhớ khởi động netcat: nc -lk 9999"
        sleep 2
        python3 src/advanced_streaming.py
        ;;
    4)
        echo "👋 Tạm biệt!"
        exit 0
        ;;
    *)
        echo "❌ Lựa chọn không hợp lệ!"
        exit 1
        ;;
esac