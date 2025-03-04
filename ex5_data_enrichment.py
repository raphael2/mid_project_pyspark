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



output_topic ='samples-enriched'
input_topic ='sensors-sample'
brokers = 'course-kafka:9092'


try:
    
    spark = SparkSession\
        .builder\
        .master("local[*]")\
        .appName('DataEnrichment')\
        .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1')\
        .getOrCreate()
    
except Exception as e:
    
    logger.error(f"Error initializing Spark: {e}")
    exit(1)

# json schema from sensors-sample topic
schema = T.StructType([
    T.StructField("event_id", T.StringType()),
    T.StructField("event_time", T.TimestampType()),
    T.StructField("car_id", T.IntegerType()),
    T.StructField("speed", T.IntegerType()),
    T.StructField("rpm", T.IntegerType()),
    T.StructField("gear", T.IntegerType())
])

# ReadStream from kafka
try:
    stream_raw = spark\
        .readStream\
        .format("kafka")\
        .option("startingOffsets", "earliest")\
        .option("kafka.bootstrap.servers", brokers)\
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


try:
    car_df= spark.read.parquet('s3a://spark/data/dims/cars')
    car_models_df = spark.read.parquet('s3a://spark/data/dims/car_models')\
        .withColumnRenamed("car_brand","brand_name")\
        .withColumnRenamed("car_model","model_name")
    
    car_colors_df = spark.read.parquet('s3a://spark/data/dims/car_colors')

    logger.info("✅ Dimension tables loaded successfully")

except Exception as e:
    logger.error(f"❌ Error loading dimension tables: {e}")
    spark.stop()
    exit(1)



enriched_df = parsed_df\
                .join(car_df, "car_id", "left")\
                .join(F.broadcast(car_models_df),"model_id")\
                .join(F.broadcast(car_colors_df),"color_id")\
                .withColumn("expected_gear", F.round(F.col("speed")/F.lit(30)))




producer = KafkaProducer(
    bootstrap_servers=brokers,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )



kafka_df = enriched_df.select(
    F.to_json(F.struct("*")).alias("value")  # Inclut toutes les colonnes
)

try:
    query = kafka_df \
        .writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", brokers) \
        .option("topic", output_topic) \
        .option("checkpointLocation", "/tmp/checkpoints3") \
        .outputMode("append")\
        .start()

    query.awaitTermination()

except Exception as e:
    logger.error(f"❌ Error streaming to Kafka: {e}")
    spark.stop()
    exit(1)