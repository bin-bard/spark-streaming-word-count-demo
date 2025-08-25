#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Spark Streaming Word Count Demo
===============================

Demo cơ bản về xử lý dữ liệu thời gian thực với Spark Streaming.
Đếm tần suất xuất hiện của các từ trong luồng dữ liệu từ socket.

Tác giả: Nhóm Big Data Applications
Ngày tạo: 2024
"""

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import sys

def create_streaming_context():
    """
    Tạo và cấu hình StreamingContext
    
    Returns:
        StreamingContext: Context để xử lý streaming
    """
    # Tạo SparkContext với tên ứng dụng
    sc = SparkContext("local[2]", "WordCountStreaming")
    sc.setLogLevel("WARN")  # Giảm log để dễ quan sát
    
    # Tạo StreamingContext với batch interval 2 giây
    ssc = StreamingContext(sc, 2)
    
    return ssc

def process_word_count(ssc, hostname="localhost", port=9999):
    """
    Xử lý word count từ socket stream
    
    Args:
        ssc: StreamingContext
        hostname: Hostname của socket server
        port: Port của socket server
    """
    try:
        # Tạo DStream từ socket
        lines = ssc.socketTextStream(hostname, port)
        
        # Áp dụng các phép biến đổi:
        # 1. flatMap: Tách mỗi dòng thành các từ
        words = lines.flatMap(lambda line: line.split(" "))
        
        # 2. map: Tạo cặp (word, 1) cho mỗi từ
        word_counts = words.map(lambda word: (word, 1))
        
        # 3. reduceByKey: Gộp các từ giống nhau và cộng dồn
        counts = word_counts.reduceByKey(lambda x, y: x + y)
        
        # In kết quả ra console
        counts.pprint()
        
        return counts
        
    except Exception as e:
        print(f"Lỗi khi xử lý stream: {e}")
        return None

def main():
    """
    Hàm chính để chạy demo
    """
    print("🚀 Khởi động Spark Streaming Word Count Demo")
    print("=" * 50)
    print("📋 Hướng dẫn:")
    print("1. Mở terminal khác và chạy: nc -lk 9999")
    print("2. Gõ các câu trong terminal netcat")
    print("3. Quan sát kết quả ở đây")
    print("4. Nhấn Ctrl+C để dừng")
    print("=" * 50)
    
    # Tạo StreamingContext
    ssc = create_streaming_context()
    
    try:
        # Xử lý word count
        word_counts = process_word_count(ssc)
        
        if word_counts is not None:
            # Bắt đầu streaming context
            ssc.start()
            
            print("✅ Streaming đã khởi động. Đang chờ dữ liệu...")
            print("💡 Chuyển sang terminal netcat và gõ các câu!")
            
            # Chờ cho đến khi bị terminate
            ssc.awaitTermination()
            
    except KeyboardInterrupt:
        print("\n🛑 Đang dừng streaming...")
        ssc.stop(stopSparkContext=True, stopGraceFully=True)
        print("✅ Đã dừng thành công!")
    except Exception as e:
        print(f"❌ Lỗi: {e}")
        ssc.stop(stopSparkContext=True, stopGraceFully=True)

if __name__ == "__main__":
    main()