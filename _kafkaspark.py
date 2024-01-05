import findspark
findspark.init()

from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
#from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
from pyspark.sql.functions import *
import json
from time import sleep

if __name__=="__main__":


    spark = SparkSession.builder.master("local").appName("Kafka Spark Project").getOrCreate()
    sc = spark.sparkContext
    ssc = StreamingContext(sc,20)
    #message = KafkaUtils.createDirectStream(ssc, topics=["testtopic"], kafkaParams={"metadata.broker.list":"localhost:9092"})
    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "testtopic") \
        .load()
    
    # Definiere das Schema für die JSON-Daten
    json_schema = StructType([
        StructField("message", StringType(), True),
        StructField("iss_position", StructType([
            StructField("longitude", StringType(), True),
            StructField("latitude", StringType(), True)
        ]), True),
        StructField("timestamp", StringType(), True)
    ])

    # Wende die Schema-Informationen auf die JSON-Daten an
    df = df.selectExpr("CAST(value AS STRING) as json_data")
    df = df.select(from_json("json_data", json_schema).alias("data"))

    # Extrahiere die Werte aus der strukturierten Spalte
    df = df.select(
        col("data.message").alias("message"),
        col("data.iss_position.longitude").cast(DoubleType()).alias("longitude"),
        col("data.iss_position.latitude").cast(DoubleType()).alias("latitude"),
        col("data.timestamp").cast(TimestampType()).alias("timestamp") 
    )

    dbOptions = {"host": "localhost", 'port': 3306,
             "user": "admin", "password": "password"}
    dbSchema = 'ISSDB'
    #windowDuration = '5 minutes'
    #slidingDuration = '1 minute'


    def saveToDatabase(batchDataframe, batchId):
        # Define function to save a dataframe to mysql
        def save_to_db(iterator):
            # Connect to database and use schema
            session = mysqlx.get_session(dbOptions)
            session.sql("USE ISStabelle").execute()

            for row in iterator:
                # Ändere die Werte in der aktuellen Zeile auf "test"
                row.id = 12345678912
                row.message = "test"
                row.latitude = "test"
                row.longitude = "test"
                row.timestamp = "test"
                # Run upsert (insert or update existing)
                sql = session.sql("INSERT INTO ISStabelle "
                                "(id, message, latitude, longitude, timestamp) VALUES (?, ?, ?, ?, ?) ")
                sql.bind(row.id, row.message, row.latitude, row.longitude, row.timestamp).execute()

            session.close()

        # Perform batch UPSERTS per data partition
        batchDataframe.foreachPartition(save_to_db)

    # Example Part 7

    dbInsertStream = df.writeStream \
        .outputMode("append") \
        .foreachBatch(saveToDatabase) \
        .start()


    # Definieren Sie die Ausgabelogik (z. B. Console oder Dateisystem)
    query = df \
        .writeStream \
        .outputMode("append") \
        .format("console") \
        .start()
        
    # Starten Sie den Streaming-Query
    query.awaitTermination()

    #data = message.map(lambda x: x[1])  # Key-Value-Pairs --> Only Value will be taken 

        #def functordd(rdd):
            #try:
                #rdd1=rdd.map(lambda x: json.loads(x))
                #df=spark.read.json(rdd1)
                #df.show()
                #df.createOrReplaceTempView("Project")
                #df1=spark.sql("SELECT iss_position.latitude,iss_position.longitude, message, timestamp FROM Project")

                #df1.write.format("csv").mode("append").save("testing")

            #except:
                #pass

        #data.foreachRDD(functordd)
        
        #ssc.start()
        #ssc.awaitTermination()