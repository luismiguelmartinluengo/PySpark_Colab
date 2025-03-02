from pyspark.sql import SparkSession
from pyspark.sql.types import *

sparkSession = SparkSession.builder.appName("example-pyspark-read-and-write").getOrCreate()

if 1 == 2:
    df = sparkSession.readStream.format("rate")\
        .option("rowsPerSecond", 10)\
        .load()

    streamQuery = df.writeStream.outputMode("append")\
                                .format("kafka")\
                                .option("kafka.bootstrap.servers", "localhost:9092")\
                                .option("topic", "topic1")\
                                .option("checkpointLocation", "output/sample_rate_checkpoint")\
                                .start()
    streamQuery.awaitTermination(20)
    streamQuery.stop()
#End if - Este código no funciona si no se tiene instalado kafka y ejecutando sobre el puerto 9092

dfmemory = sparkSession.readStream.format("rate")\
    .option("rowsPerSecond", 10)\
    .load()
streamQueryMemory = dfmemory.writeStream.outputMode("append")\
                            .format("memory")\
                            .queryName("memoryQuery")\
                            .start()
streamQueryMemory.awaitTermination(10)
streamQueryMemory.stop()

dfMemoryQuery = sparkSession.sql("select * from memoryQuery")
dfMemoryQuery.show(truncate=False)
