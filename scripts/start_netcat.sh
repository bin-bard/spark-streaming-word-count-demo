#!/bin/bash

# Script để khởi động netcat server cho demo
# Tác giả: Nhóm Big Data Applications

echo "🚀 Khởi động Netcat Server cho Spark Streaming Demo"
echo "================================================="

# Kiểm tra xem netcat có được cài đặt không
if ! command -v nc &> /dev/null; then
    echo "❌ Netcat chưa được cài đặt!"
    echo "📦 Cách cài đặt:"
    echo "   Ubuntu/Debian: sudo apt-get install netcat"
    echo "   CentOS/RHEL: sudo yum install nc"
    echo "   MacOS: brew install netcat"
    echo "   Windows: Sử dụng WSL hoặc tải Nmap (có kèm ncat)"
    exit 1
fi

# Cổng mặc định
PORT=${1:-9999}

echo "📡 Khởi động netcat server trên port $PORT..."
echo "💡 Sử dụng: $0 [port] (mặc định port 9999)"
echo "📋 Hướng dẫn:"
echo "   1. Gõ các câu và nhấn Enter"
echo "   2. Dữ liệu sẽ được gửi đến Spark Streaming"
echo "   3. Nhấn Ctrl+C để dừng"
echo "================================================="
echo "✅ Server đã sẵn sàng. Bắt đầu gõ các câu:"

# Khởi động netcat với keep-alive
nc -lk $PORT