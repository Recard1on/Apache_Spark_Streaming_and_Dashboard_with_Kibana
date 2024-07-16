# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from pyspark.sql.functions import from_json, col, sum as spark_sum
from elasticsearch import Elasticsearch

spark = SparkSession \
    .builder \
    .appName("Streaming from Kafka") \
    .getOrCreate()

df = spark.readStream\
    .format("kafka") \
    .option("kafka.bootstrap.servers", "164.92.196.12:9092") \
    .option("subscribe", "Finans") \
    .load()

schema = StructType([
    StructField("pid", StringType(), True),
    StructField("ptype", StringType(), True),
    StructField("sender", StructType([
        StructField("bank", StringType(), True),
        StructField("iban", StringType(), True),
        StructField("name", StringType(), True),
        StructField("city", StringType(), True)
    ]), True),
    StructField("receiver", StructType([
        StructField("bank", StringType(), True),
        StructField("iban", StringType(), True),
        StructField("name", StringType(), True),
        StructField("city", StringType(), True)
    ]), True),
    StructField("balance", IntegerType(), True),
    StructField("btype", StringType(), True),
    StructField("timestamp", StringType(), True)
])

df_writing = df.select(from_json(col('value').cast('string'), schema).alias('data')).select('data.*')



def writeToElasticsearch(df, epoch_id):
    df.write \
        .format("org.elasticsearch.spark.sql") \
        .option("es.nodes", "164.92.196.12") \
        .option("es.port", "9200") \
        .option("es.resource", "financial_data/_doc") \
        .mode("append") \
        .save()

streamingQuery = df_writing.writeStream \
    .option("numRows", 4) \
    .option("truncate", False) \
    .outputMode("append") \
    .foreachBatch(writeToElasticsearch) \
    .start()




