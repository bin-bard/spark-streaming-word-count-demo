import sys
from pyspark import SparkContext
from pyspark.streaming import StreamingContext

if __name__ == "__main__":
    # 1. Tạo SparkContext và StreamingContext
    # Tạo SparkContext với master là cụm Spark trong Docker và tên app là "StreamingWordCountFilter"
    sc = SparkContext("spark://spark-master:7077", "StreamingWordCountFilter")
    
    # Tạo StreamingContext với batch interval là 2 giây
    ssc = StreamingContext(sc, 2)

    # 2. Tạo DStream từ nguồn dữ liệu (socket)
    # Lắng nghe dữ liệu từ TCP socket trên host spark-master, port 9999
    lines = ssc.socketTextStream("spark-master", 9999)

    # 3. Áp dụng các phép biến đổi trên DStream
    # Tách mỗi dòng thành các từ
    words = lines.flatMap(lambda line: line.split(" "))
    
    # Lọc chỉ lấy các từ có độ dài lớn hơn 4 ký tự
    long_words = words.filter(lambda word: len(word) > 4)
    
    # Tạo cặp (word, 1) cho mỗi từ
    pairs = long_words.map(lambda word: (word, 1))
    
    # Đếm số lần xuất hiện của mỗi từ trong từng batch
    wordCounts = pairs.reduceByKey(lambda x, y: x + y)

    # 4. In kết quả ra console
    wordCounts.pprint()

    # 5. Bắt đầu xử lý luồng dữ liệu
    ssc.start()
    ssc.awaitTermination()
