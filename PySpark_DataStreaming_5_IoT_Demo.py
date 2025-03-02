
from pyspark.sql import SparkSession
from pyspark.sql.functions import * 
from pyspark.sql.types import *

sparkSession = SparkSession.builder.appName("IoT_Demo")\
      .config("spark.sql.shuffle.partitions", "200") \
      .config("spark.default.parallelism", "200") \
      .config("spark.streaming.backpressure.enabled", "true") \
      .getOrCreate()

schema = StructType([StructField("serial_number", StringType(), True),
                     StructField("temperature", DoubleType(), True),
                     StructField("humidity", DoubleType(), True),
                     StructField("timestamp", TimestampType(), True)])

iotDataPath = './iot_data/'

dfStream = sparkSession.readStream.format('csv')\
                            .schema(schema)\
                            .option('header','true')\
                            .option('maxFilesPerTrigger', 1)\
                            .load(iotDataPath)

dfStreamWithWatermark = dfStream.withWatermark("timestamp", "1 hour")

dfWindowedAgg = dfStreamWithWatermark.groupBy(window(dfStreamWithWatermark.timestamp, "1 hour").alias('hourly_window'))\
                        .agg(avg(dfStreamWithWatermark.temperature).alias('avg_temperature'),
                              avg(dfStreamWithWatermark.humidity).alias('avg_humidity'))\
                        .select(date_format('hourly_window.start', 'yyyy-MM-dd HH:mm:ss').alias('Hour'),
                                'avg_temperature',
                                'avg_humidity')
'''
streamQueryConsole = dfWindowedAgg.writeStream.outputMode('complete')\
                                    .format('console')\
                                    .option('truncate', 'false')\
                                    .start()
streamQueryConsole.awaitTermination(15)
streamQueryConsole.stop()
'''
streamQueryCsv = dfWindowedAgg.writeStream.outputMode('append')\
                                    .format('csv')\
                                    .option('path', './output/iot_data_complete')\
                                    .option('checkpointLocation', './checkpoint/')\
                                    .start()
streamQueryCsv.awaitTermination(60)
streamQueryCsv.stop()
