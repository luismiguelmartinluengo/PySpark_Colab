from pyspark.sql import SparkSession
from pyspark.sql.types import *

sparkSession = SparkSession.builder.appName("example-pyspark-read-and-write").getOrCreate()

'''
Para conectar con un socket, se debe especificar el host y el puerto
df = sparkSession.readStream.format("socket")\
    .option("host", "localhost")\
    .option("port", 9999)\
    .load()

Para conectar con kafka, se debe especificar el host y el puerto
df = sparkSession.readStream.format("kafka")\
    .option("kafka.bootstrap.servers", "localhost:9092")\
    .option("subscribe", "topic")\
    .load()
'''

df = sparkSession.readStream.format("rate")\
    .option("rowsPerSecond", 10)\
    .load()
streamingQuery = df.writeStream.outputMode("append")\
                                    .format("json")\
                                    .option("path", "output/sample_rate")\
                                    .option("checkpointLocation", "output/sample_rate_checkpoint")\
                                    .trigger(processingTime="10 seconds")\
                                    .start()
streamingQuery.awaitTermination(20)
streamingQuery.stop()