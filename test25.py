from ipaddress import collapse_addresses
from threading import Thread
import findspark
findspark.init("/home/vm/spark") 
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
import pandas as pd
import numpy as np
import json
from pyspark.ml import Pipeline
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from sparknlp.annotator import *
from sparknlp.base import *
import sparknlp
from sparknlp.pretrained import PretrainedPipeline
import pyspark.sql.functions as f
from pyspark.sql.functions import *
import io
import time
import os
import csv
import pymongo
from pyspark.sql.types import *


myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["tweets_analysis"]
mycol = mydb["tweets"]


spark = SparkSession.builder\
    .appName("Spark NLP")\
    .master("local[*]")\
    .config("spark.driver.memory","4G")\
    .config("spark.driver.maxResultSize", "0") \
    .config("spark.kryoserializer.buffer.max", "2000M")\
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")\
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1")\
    .config("spark.jars", "/home/vm/rt_tweets_analysis/spark-nlp-assembly-4.2.6.jar, /home/vm/rt_tweets_analysis/mongo-spark-connector_2.12-10.1.0.jar")\
    .getOrCreate()


# Create DataFrame representing the stream of input lines from connection to localhost:5555
lines = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "tweets") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

tweets = lines.selectExpr("CAST(value AS STRING)")
schema = StructType([
    StructField("text", StringType(), True),
    StructField("created_at", TimestampType(), True)
])
tweets = tweets.select(from_json(tweets.value, schema).alias("value")).select("value.text", "value.created_at")



#load the pretrained model
nlpPipeline = PipelineModel.load("/home/vm/analyze_sentimentdl_use_twitter_en/")
result = nlpPipeline.transform(tweets)


# result = result.select(F.explode(F.arrays_zip('document.result', 'sentiment.result')).alias("cols"),'created_at') \
# .select(F.expr("cols['0']").alias("document"),
#         F.expr("cols['1']").alias("sentiment"),
#         'created_at')
result = result.select(F.expr("sentence_embeddings[0]['result']").alias("document"), F.expr("sentiment[0]['result']").alias("sentiment"), 'created_at')




# # tumbling window
# windowedSentimentsCounts = \
#   result \
#     .withWatermark("created_at", "5 minutes") \
#     .groupBy("sentiment", window("created_at", "1 minutes")) \
#     .count() \


# #sliding window
# windowedSentimentsCounts = \
#     result \
#         .withWatermark("created_at", "2 minutes") \
#         .groupBy("sentiment", window("created_at", "2 minutes", "30 seconds")) \
#         .count() \

# tumbling window
windowedSentimentsCounts = \
  result \
    .withWatermark("created_at", "1 minutes") \
    .groupBy("sentiment", window("created_at", "1 minutes")) \
    .count() \



windowedSentimentsCounts \
        .select("window.start", "window.end", "sentiment", "count") \
        .createOrReplaceTempView("stream")




query = spark.sql("select * from stream") \
    .selectExpr("CAST(stream.start AS STRING) AS key", "to_json(struct(*)) AS value") \
    .writeStream \
    .outputMode("update") \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "sentiments") \
    .option("checkpointLocation", "/home/vm/rt_tweets_analysis/checkpoint") \
    .trigger(processingTime='1 minutes') \
    .start()

# def process_row(row):
#     myclient = pymongo.MongoClient("mongodb://localhost:27017/")
#     mydb = myclient["tweets_analysis"]
#     mycol = mydb["tweets"]
        
#     # Write row to storage
#     print(row)
#     mydict = {
#         "start": row[0],
#         "end": row[1],
#         "sentiment": row[2],
#         "count": row[3]
#     }
#     x = mycol.insert_one(mydict)
#     print(str(x.inserted_id) + " inserted \n")


# query = spark.sql("select * from stream")\
#     .writeStream \
#     .foreach(process_row(Row, myclient, mydb, mycol)) \
#     .outputMode("update") \
#     .trigger(processingTime='1 minutes') \
#     .start()

# query = spark.sql("select * from stream") \
#     .writeStream \
#     .outputMode("update") \
#     .format("mongodb") \
#     .option("checkpointLocation", "/home/vm/rt_tweets_analysis/checkpoint") \
#     .option("forceDeleteTempCheckpointLocation", "true") \
#     .option("spark.mongodb.connection.uri", "mongodb://localhost:27017/") \
#     .option("spark.mongodb.database", "tweets_analysis") \
#     .option("spark.mongodb.collection", "tweets") \
#     .trigger(processingTime='1 minutes') \
#     .start()

query.awaitTermination()