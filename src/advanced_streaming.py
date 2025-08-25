#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Spark Streaming Advanced Demo - Windowing & Top Words
=====================================================

Demo nâng cao với windowing operations và top words analysis.
Hiển thị top 5 từ xuất hiện nhiều nhất trong sliding window.

Tác giả: Nhóm Big Data Applications
Ngày tạo: 2024
"""

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import sys

def create_streaming_context():
    """
    Tạo và cấu hình StreamingContext với checkpoint
    
    Returns:
        StreamingContext: Context để xử lý streaming
    """
    # Tạo SparkContext với tên ứng dụng
    sc = SparkContext("local[2]", "AdvancedWordCountStreaming")
    sc.setLogLevel("WARN")  # Giảm log để dễ quan sát
    
    # Tạo StreamingContext với batch interval 5 giây
    ssc = StreamingContext(sc, 5)
    
    # Thiết lập checkpoint để lưu trữ state (cần cho windowing)
    ssc.checkpoint("checkpoint")
    
    return ssc

def process_windowed_word_count(ssc, hostname="localhost", port=9999):
    """
    Xử lý word count với windowing và top words analysis
    
    Args:
        ssc: StreamingContext
        hostname: Hostname của socket server
        port: Port của socket server
    """
    try:
        # Tạo DStream từ socket
        lines = ssc.socketTextStream(hostname, port)
        
        # Xử lý cơ bản: tách từ và tạo pairs
        words = lines.flatMap(lambda line: line.split(" "))
        word_pairs = words.map(lambda word: (word.lower().strip(), 1))
        
        # 🎯 WINDOWING OPERATIONS
        # Sliding window: 30 giây, slide mỗi 10 giây
        windowed_word_counts = word_pairs.reduceByKeyAndWindow(
            lambda x, y: x + y,      # Hàm reduce
            lambda x, y: x - y,      # Hàm inverse reduce (để tối ưu)
            windowDuration=30,       # Window size: 30 giây
            slideDuration=10         # Slide interval: 10 giây
        )
        
        # 🏆 TOP WORDS ANALYSIS
        def get_top_words(time, rdd):
            """Lấy và hiển thị top 5 từ trong window hiện tại"""
            try:
                # Lọc bỏ từ rỗng và sắp xếp theo count giảm dần
                top_words = rdd.filter(lambda x: len(x[0]) > 0) \
                              .filter(lambda x: x[0] != '') \
                              .takeOrdered(5, key=lambda x: -x[1])
                
                if top_words:
                    print(f"\n🏆 TOP 5 TỪ TRONG WINDOW 30 GIÂY (tại {time})")
                    print("=" * 50)
                    for i, (word, count) in enumerate(top_words, 1):
                        print(f"{i}. {word:15} -> {count:3} lần")
                    print("=" * 50)
                else:
                    print(f"\n📭 Không có dữ liệu trong window (tại {time})")
                    
            except Exception as e:
                print(f"❌ Lỗi khi xử lý top words: {e}")
        
        # Áp dụng hàm phân tích cho mỗi window
        windowed_word_counts.foreachRDD(get_top_words)
        
        # 📊 REAL-TIME STATS
        def print_stats(time, rdd):
            """In thống kê real-time"""
            try:
                total_words = rdd.map(lambda x: x[1]).reduce(lambda x, y: x + y) if not rdd.isEmpty() else 0
                unique_words = rdd.count()
                
                print(f"\n📊 THỐNG KÊ WINDOW (tại {time})")
                print(f"   📝 Tổng số từ: {total_words}")
                print(f"   🔤 Từ duy nhất: {unique_words}")
                print(f"   📈 Tỷ lệ lặp: {total_words/unique_words:.2f}" if unique_words > 0 else "   📈 Tỷ lệ lặp: N/A")
                
            except Exception as e:
                print(f"❌ Lỗi khi tính thống kê: {e}")
        
        windowed_word_counts.foreachRDD(print_stats)
        
        return windowed_word_counts
        
    except Exception as e:
        print(f"❌ Lỗi khi xử lý stream: {e}")
        return None

def main():
    """
    Hàm chính để chạy demo nâng cao
    """
    print("🚀 Spark Streaming Advanced Demo - Windowing & Analytics")
    print("=" * 65)
    print("🎯 Tính năng nâng cao:")
    print("   ✅ Sliding Window (30s window, 10s slide)")
    print("   ✅ Top 5 từ phổ biến nhất trong mỗi window")
    print("   ✅ Thống kê real-time (tổng từ, từ duy nhất)")
    print("   ✅ Inverse reduce để tối ưu performance")
    print("=" * 65)
    print("📋 Hướng dẫn:")
    print("1. Mở terminal khác và chạy: nc -lk 9999")
    print("2. Gõ nhiều câu liên tục trong 30 giây")
    print("3. Quan sát top words và thống kê mỗi 10 giây")
    print("4. Nhấn Ctrl+C để dừng")
    print("=" * 65)
    print("💡 Ví dụ input để test:")
    print("   'spark streaming is awesome'")
    print("   'spark processing real time data'")  
    print("   'streaming with spark is fun'")
    print("=" * 65)
    
    # Tạo StreamingContext
    ssc = create_streaming_context()
    
    try:
        # Xử lý windowed word count
        word_counts = process_windowed_word_count(ssc)
        
        if word_counts is not None:
            # Bắt đầu streaming context
            ssc.start()
            
            print("✅ Advanced Streaming đã khởi động...")
            print("⏰ Đang chờ dữ liệu... (kết quả sẽ xuất hiện sau 10 giây)")
            print("🎯 Gõ nhiều câu để thấy top words analysis!")
            
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