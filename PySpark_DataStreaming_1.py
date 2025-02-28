from pyspark.sql import SparkSession
from pyspark.sql.functions import split, explode
import os

print('Hola Mundo')


sparkSession = SparkSession.builder.appName("PySpark - DataStreaming 1").master("local[*]").getOrCreate()
sparkSession.sparkContext.setLogLevel("WARN")

schema = "value STRING"
dataFrame = sparkSession.readStream.schema(schema).format("text").load("./data_streaming/")
dfWordsList = dataFrame.select(split(dataFrame.value, " ").alias("words"))
dfWords = dfWordsList.select(explode(dfWordsList.words).alias("word"))
streamingQuery = dfWords.writeStream.outputMode("append")\
                                    .format("console")\
                                    .start()
streamingQuery.awaitTermination(60)
streamingQuery.stop()
