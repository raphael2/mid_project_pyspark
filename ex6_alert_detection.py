from pyspark.sql import SparkSession
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from kafka import KafkaProducer
import json 
import logging


# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)



input_topic ='samples-enriched'
output_topic ='alert-data'
broker = 'course-kafka:9092'

try:
    spark = SparkSession\
        .builder\
        .master("local[*]")\
        .appName('DataEnrichment')\
        .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1')\
        .getOrCreate()
    logger.info("✅ Spark Session initialized")

except Exception as e:
    logger.error(f"❌ Error initializing Spark: {e}")
    exit(1)


# json schema from sensors-sample topic
schema = T.StructType([
    T.StructField("event_id", T.StringType()),
    T.StructField("event_time", T.TimestampType()),
    T.StructField("car_id", T.IntegerType()),
    T.StructField("speed", T.IntegerType()),
    T.StructField("rpm", T.IntegerType()),
    T.StructField("gear", T.IntegerType()),
    T.StructField("brand_name", T.StringType()),
    T.StructField("model_name", T.StringType()),
    T.StructField("color_name", T.StringType()),
    T.StructField("expected_gear", T.FloatType())
])



# ReadStream from kafka
try:
    stream_raw = spark\
        .readStream\
        .format("kafka")\
        .option("startingOffsets", "earliest")\
        .option("kafka.bootstrap.servers", broker)\
        .option("subscribe", input_topic)\
        .load()

    logger.info("✅ Kafka stream initialized")

except Exception as e:
    logger.error(f"❌ Error initializing Kafka stream: {e}")
    spark.stop()
    exit(1)


stream_df = stream_raw.select(
    F.expr("CAST(value AS STRING)").alias("json_value")
)

parsed_df = stream_df\
    .withColumn('parsed_json', F.from_json(F.col('json_value'), schema)) \
    .select(F.col('parsed_json.*'))



filter_df = parsed_df.filter(
            (F.col("speed") > F.lit(120)) | 
            (F.col("expected_gear") != F.col("gear")) | 
            (F.col("rpm") > F.lit(6000))
        ) 



# Convert DataFrame into Kafka-compatible format
kafka_df = filter_df.select(
    F.to_json(F.struct("*")).alias("value")  
)


try:
    query = kafka_df \
        .writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", broker) \
        .option("topic", output_topic) \
        .option("checkpointLocation", "/tmp/checkpoints/alert_detection5") \
        .outputMode("append")\
        .start()
    
    logger.info(f"✅ Streaming alerts to Kafka topic '{output_topic}' started")
    query.awaitTermination()

except Exception as e:
    logger.error(f"❌ Error streaming to Kafka: {e}")
    spark.stop()
    exit(1)