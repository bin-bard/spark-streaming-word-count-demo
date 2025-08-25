# Spark Streaming Word Count Demo

## 📖 Giới thiệu

Demo này minh họa việc xử lý dữ liệu thời gian thực với **Apache Spark Streaming** thông qua bài toán **Word Count** kinh điển.

### 🎯 Mục tiêu
- Hiểu kiến trúc Micro-batching của Spark Streaming
- Làm quen với khái niệm DStream (Discretized Stream)
- Thực hành xây dựng ứng dụng streaming đơn giản
- So sánh DStream với RDD

### 🏗️ Kiến trúc Spark Streaming
```
Data Stream → Micro-batches → RDD → Processing → Output
```

## 🚀 Cài đặt

### Yêu cầu hệ thống
- Java 8+
- Python 3.7+
- Apache Spark 3.x
- PySpark

### Cài đặt dependencies
```bash
pip install pyspark
```

### Cài đặt netcat (để tạo data stream)
**Windows:**
```bash
# Sử dụng Windows Subsystem for Linux (WSL)
# Hoặc tải Nmap (có kèm ncat)
```

**Linux/MacOS:**
```bash
# Thường có sẵn, hoặc:
sudo apt-get install netcat  # Ubuntu/Debian
brew install netcat          # MacOS
```

## 🎮 Cách chạy Demo

### Bước 1: Khởi động Data Stream Server
Mở terminal đầu tiên và chạy:
```bash
nc -lk 9999
```

### Bước 2: Chạy Spark Streaming Application
Mở terminal thứ hai và chạy:
```bash
python word_count_streaming.py
```

### Bước 3: Gửi dữ liệu
Trong terminal đầu tiên (netcat), gõ các câu:
```
hello world spark streaming
spark is awesome
real time data processing
```

### Bước 4: Quan sát kết quả
Terminal thứ hai sẽ hiển thị:
```
-------------------------------------------
Time: 2024-XX-XX XX:XX:XX
-------------------------------------------
(hello, 1)
(world, 1)
(spark, 2)
(streaming, 1)
(is, 1)
(awesome, 1)
...
```

## 📁 Cấu trúc Project

```
spark-streaming-word-count-demo/
├── README.md
├── requirements.txt
├── src/
│   ├── word_count_streaming.py      # Demo cơ bản
│   ├── word_count_filtered.py       # Đếm từ > 4 ký tự
│   └── advanced_streaming.py        # Demo nâng cao
├── data/
│   └── sample_data.txt             # Dữ liệu mẫu
├── docs/
│   ├── architecture.md             # Kiến trúc chi tiết
│   └── dstream_vs_rdd.md          # So sánh DStream vs RDD
└── scripts/
    ├── start_netcat.sh            # Script khởi động netcat
    └── run_demo.sh                # Script chạy demo
```

## 🧪 Các Demo

### 1. Word Count cơ bản
```bash
python src/word_count_streaming.py
```

### 2. Word Count với filter (từ > 4 ký tự)
```bash
python src/word_count_filtered.py
```

### 3. Demo nâng cao với windowing
```bash
python src/advanced_streaming.py
```

## 💡 Kiến thức chính

### DStream (Discretized Stream)
- **Khái niệm**: Chuỗi liên tục các RDD được tạo ra từ data stream
- **Đặc điểm**: Immutable, fault-tolerant, distributed
- **Tạo ra**: Từ data sources như sockets, files, Kafka, etc.

### So sánh DStream vs RDD

| Đặc điểm | DStream | RDD |
|----------|---------|-----|
| **Tính chất** | Streaming, continuous | Batch, static |
| **Thời gian** | Real-time processing | Batch processing |
| **Cấu trúc** | Sequence of RDDs | Single distributed dataset |
| **Operations** | Transformations + Output operations | Transformations + Actions |

### Kiến trúc Micro-batching
1. **Input**: Data stream được chia thành micro-batches
2. **Processing**: Mỗi micro-batch được xử lý như một RDD
3. **Output**: Kết quả được ghi ra output systems

## 🔄 Tương tác

### Thử thách 1: Filter từ dài
Sửa đổi code để chỉ đếm các từ có độ dài > 4 ký tự.

### Thử thách 2: Top Words
Hiển thị top 5 từ xuất hiện nhiều nhất trong mỗi batch.

### Thử thách 3: Windowing
Đếm từ trong window 30 giây, cập nhật mỗi 10 giây.

## 📚 Tài liệu tham khảo

- [Apache Spark Streaming Programming Guide](https://spark.apache.org/docs/latest/streaming-programming-guide.html)
- [DStream Documentation](https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.streaming.DStream.html)

## 👥 Nhóm thực hiện

- **Hoàng**: Tổng quan & Lý thuyết
- **Năng**: DStream & So sánh với RDD  
- **Tài + An**: Demo & Phân tích code
- **Hưng**: Tương tác & Tổng kết

---

🎓 **Big Data Applications - Real-time Data Streaming**