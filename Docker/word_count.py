from pyspark import SparkContext
from pyspark.streaming import StreamingContext

sc = SparkContext("local[*]", "NetworkWordCount")
ssc = StreamingContext(sc, 2)

# Kết nối tới DStream bằng tên service 'netcat-server' trong mạng Docker
lines = ssc.socketTextStream("netcat-server", 9999)

words = lines.flatMap(lambda line: line.split(" "))
pairs = words.map(lambda word: (word, 1))
wordCounts = pairs.reduceByKey(lambda x, y: x + y)

wordCounts.pprint()

ssc.start()
ssc.awaitTermination()