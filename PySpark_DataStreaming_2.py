from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import sqlite3
import logging


print('Hola Mundo')

#Configuración de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

#Inicializar Sesión de spark
sparkSession = SparkSession.builder.appName("PySpark_DataStreaming_2")\
                                    .master("local[4]")\
                                    .config("spark.sql.shuffle.partitions", 2)\
                                    .config("spark.streaming.stopGracefullyOnShutdown", "true")\
                                    .getOrCreate()

df = sparkSession.read.csv("./iot_data/iot_data_day_1.csv",
                           header = True,
                           inferSchema = True)
dfAgg = df.select("serial_number", "temperature").groupBy("serial_number").agg(avg("temperature").alias("avg_temperature"))
dfAgg.write.save("./output/iot_data_day_1_agg", 
                 format="csv", 
                 mode="overwrite",
                 header=True)

def writeToSqlite3(df, batch_id):
    databasePath = "./output/iot_data_agg.db"
    logger.info(f"Writing batch {batch_id} to sqlite3 database at {databasePath}")
    connection = sqlite3.connect(databasePath)
    df.toPandas().to_sql("iot_data_agg", connection, if_exists="append", index=False)
    connection.close()
#End writeToSqlite3

writeToSqlite3(dfAgg, None)
logger.info("Proceso finalizado")
logger.info(f'Se han procesado {dfAgg.count()} registros')
sparkSession.stop()