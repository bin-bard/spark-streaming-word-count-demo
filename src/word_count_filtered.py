#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Spark Streaming Word Count Demo - Filtered Version
==================================================

Demo tương tác: Chỉ đếm các từ có độ dài lớn hơn 4 ký tự.
Đây là bài tập tương tác cho các nhóm khác.

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
    sc = SparkContext("local[2]", "WordCountFilteredStreaming")
    sc.setLogLevel("WARN")  # Giảm log để dễ quan sát
    
    # Tạo StreamingContext với batch interval 2 giây
    ssc = StreamingContext(sc, 2)
    
    return ssc

def process_filtered_word_count(ssc, hostname="localhost", port=9999, min_length=4):
    """
    Xử lý word count từ socket stream với filter độ dài từ
    
    Args:
        ssc: StreamingContext
        hostname: Hostname của socket server
        port: Port của socket server
        min_length: Độ dài tối thiểu của từ (mặc định = 4)
    """
    try:
        # Tạo DStream từ socket
        lines = ssc.socketTextStream(hostname, port)
        
        # Áp dụng các phép biến đổi:
        # 1. flatMap: Tách mỗi dòng thành các từ
        words = lines.flatMap(lambda line: line.split(" "))
        
        # 2. filter: Chỉ giữ lại các từ có độ dài > min_length
        # 🎯 ĐÂY LÀ ĐIỂM KHÁC BIỆT CHÍNH!
        filtered_words = words.filter(lambda word: len(word) > min_length)
        
        # 3. map: Tạo cặp (word, 1) cho mỗi từ đã lọc
        word_counts = filtered_words.map(lambda word: (word, 1))
        
        # 4. reduceByKey: Gộp các từ giống nhau và cộng dồn
        counts = word_counts.reduceByKey(lambda x, y: x + y)
        
        # In thông tin filter và kết quả
        def print_results(time, rdd):
            taken = rdd.take(10)  # Lấy 10 kết quả đầu tiên
            if taken:
                print(f"\n⏰ Thời gian: {time}")
                print(f"🔍 Chỉ hiển thị từ có độ dài > {min_length} ký tự:")
                print("-" * 40)
                for word, count in taken:
                    print(f"📝 {word} ({len(word)} ký tự): {count} lần")
                print("-" * 40)
            else:
                print(f"\n⏰ Thời gian: {time}")
                print("📭 Không có từ nào thỏa mãn điều kiện")
        
        counts.foreachRDD(print_results)
        
        return counts
        
    except Exception as e:
        print(f"❌ Lỗi khi xử lý stream: {e}")
        return None

def main():
    """
    Hàm chính để chạy demo
    """
    print("🎯 Spark Streaming Word Count - Filtered Version")
    print("=" * 60)
    print("📋 Tính năng đặc biệt:")
    print("   ✅ Chỉ đếm các từ có độ dài > 4 ký tự")
    print("   ✅ Hiển thị độ dài từ trong kết quả")
    print("   ✅ Lọc bỏ các từ ngắn (a, an, the, is, ...)")
    print("=" * 60)
    print("📋 Hướng dẫn:")
    print("1. Mở terminal khác và chạy: nc -lk 9999")
    print("2. Gõ các câu có từ dài và ngắn")
    print("3. Quan sát chỉ từ dài được đếm")
    print("4. Nhấn Ctrl+C để dừng")
    print("=" * 60)
    print("💡 Ví dụ input tốt:")
    print("   'hello world programming streaming'")
    print("   'apache spark streaming processing'")
    print("=" * 60)
    
    # Tạo StreamingContext
    ssc = create_streaming_context()
    
    try:
        # Xử lý word count với filter
        word_counts = process_filtered_word_count(ssc, min_length=4)
        
        if word_counts is not None:
            # Bắt đầu streaming context
            ssc.start()
            
            print("✅ Filtered Streaming đã khởi động. Đang chờ dữ liệu...")
            print("🔍 Chỉ từ có độ dài > 4 ký tự sẽ được đếm!")
            
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