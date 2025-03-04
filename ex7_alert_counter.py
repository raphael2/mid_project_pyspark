from pyspark.sql import SparkSession
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
import logging



# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)



input_topic ='alert-data'
broker = 'course-kafka:9092'

# Initialize Spark session
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



# json schema from alert-data topic
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


counter_df = parsed_df\
    .withWatermark("event_time", "15 minutes") \
    .groupBy(F.window(F.col("event_time"), "15 minutes")) \
    .agg(
            F.count("*").alias("num_of_rows"),
            F.count(F.when(F.lower(F.col("color_name")) == "black",1)).alias("num_of_black"),
            F.count(F.when(F.lower(F.col("color_name")) == "white",1)).alias("num_of_white"),
            F.count(F.when(F.lower(F.col("color_name")) == "silver",1)).alias("num_of_silver"),
            F.max("speed").alias("maximum_speed"),
            F.max("gear").alias("maximum_gear"),
            F.max("rpm").alias("maximum_rpm")
         )
# WriteStream to console for monitoring
try:
    query = counter_df \
    .orderBy(F.col("window.start"))\
    .writeStream \
    .outputMode("complete")\
    .format("console") \
    .option("truncate", "false") \
    .start()

    logger.info("✅ Streaming alert counters to console started")
    query.awaitTermination()
except Exception as e:
    logger.error(f"❌ Error streaming to console: {e}")
    spark.stop()
    exit(1)