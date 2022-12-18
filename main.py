from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import math
import string
import random
import  os

KAFKA_INPUT_TOPIC_NAME_CONS = "linuxhint1"
KAFKA_BOOTSTRAP_SERVERS_CONS = "77957721c61c.mylabserver.com:9092"
MALL_LONGITUDE=78.446841
MALL_LATITUDE=17.427229
MALL_THRESHOLD_DISTANCE=100

if __name__ == "__main__":
    #sys.path.append("/Users/shuvamoy/Downloads/spark-sql-kafka-0-10_2.11-2.4.0.jar")
    print("PySpark Structured Streaming with Kafka Application Started …")
    #os.environ["JAVA_HOME"]="/Users/shuvamoy/Downloads/jdk1.8.0_202"
    os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-hadoop-cloud_2.12:3.2.0,org.apache.spark:spark-streaming-kafka-0-10_2.12:3.3.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1 pyspark-shell'

    spark = SparkSession \
    .builder \
    .appName("PySpark Structured Streaming with Kafka") \
    .master("local[*]") \
    .getOrCreate()
    spark.conf.set("spark.sql.streaming.checkpointLocation","s3a://shuvabuc007/checkpoint/")
    #spark.sparkContext._jsc.hadoopConfiguration().set("com.amazonaws.services.s3.enableV4", "true")
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    #spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.aws.credentials.provider", \
    #                                                 "com.amazonaws.auth.InstanceProfileCredentialsProvider,com.amazonaws.auth.DefaultAWSCredentialsProviderChain")
    #spark.sparkContext._jsc.hadoopConfiguration().set("fs.AbstractFileSystem.s3a.impl", "org.apache.hadoop.fs.s3a.S3A")

    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.access.key", "AKIAQUISU65OWAK5P6UM")
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "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")
    spark.sparkContext._jsc.hadoopConfiguration().set("fs.s3a.endpoint", "s3.us-east-1.amazonaws.com")

    print('v', spark.version)
    stream_detail_df = spark \
        .readStream \
        .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS_CONS) \
            .option("subscribe", KAFKA_INPUT_TOPIC_NAME_CONS) \
            .option("startingOffsets", "latest") \
            .load()

    sample_schema = (
        StructType()
        .add("col_a", StringType())
        .add("col_b", StringType())
    )

    stream_detail_df = stream_detail_df.selectExpr("CAST(value AS STRING)","timestamp")
    info_dataframe = stream_detail_df.select(
        from_json(col("value"), sample_schema).alias("sample"), "timestamp"
    )
    # .format("console").start().awaitTermination()
    rawQuery = info_dataframe \
        .writeStream \
        .format("json").outputMode("append").option("path", "s3a://shuvabuc007/").start().awaitTermination()
    # .format("console").start().awaitTermination()

