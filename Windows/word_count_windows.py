from pyspark import SparkContext
from pyspark.streaming import StreamingContext

# Khởi tạo Spark Context
sc = SparkContext("local[2]", "NetworkWordCount")

# Tạo StreamingContext, xử lý dữ liệu theo từng batch 2 giây.
ssc = StreamingContext(sc, 2)

# Tạo DStream, nhận dữ liệu văn bản từ socket localhost:9999 (netcat).
lines = ssc.socketTextStream("localhost", 9999)

# flatMap: Tách mỗi dòng nhận được thành các từ riêng lẻ.
# Ví dụ: "hello world" -> "hello", "world"
words = lines.flatMap(lambda line: line.split(" "))

# map: Chuyển mỗi từ thành một cặp (từ, 1) để chuẩn bị đếm.
# Ví dụ: "hello" -> ("hello", 1)
pairs = words.map(lambda word: (word, 1))

# reduceByKey: Đếm tần suất bằng cách cộng các số 1 của các từ trùng nhau.
# Ví dụ: ("hello", 1), ("hello", 1) -> ("hello", 2)
wordCounts = pairs.reduceByKey(lambda x, y: x + y)

# In kết quả của mỗi batch ra màn hình.
wordCounts.pprint()

# Bắt đầu quá trình xử lý streaming.
ssc.start()
# Chờ cho đến khi tiến trình bị ngắt.
ssc.awaitTermination()
