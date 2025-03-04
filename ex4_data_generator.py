from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from kafka import KafkaProducer
import json
import time 
import logging



# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# Kafka configuration
topic ='sensors-sample'
brokers = ['course-kafka:9092']


# Initialize Spark session
try:
    spark = SparkSession.builder\
        .master("local[*]")\
        .appName("DataGenerator")\
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2")\
        .getOrCreate()

    logger.info("Spark Session initialized")

except Exception as e:
    logger.error(f"Error initializing Spark: {e}")
    exit(1)  # Stop execution if Spark initialization fails



try:
    cars = spark.read.parquet("s3a://spark/data/dims/cars")
    logger.info("Cars data loaded successfully")
except Exception as e:
    logger.error(f"Error loading cars data: {e}")
    spark.stop()
    exit(1)



cars_df = cars.select("car_id") \
    .withColumn("event_id", F.expr("uuid()")) \
    .withColumn("event_time", F.current_timestamp().cast(T.StringType())) \
    .withColumn("speed", (F.rand() * F.lit(200)).cast(T.IntegerType())) \
    .withColumn("rpm", (F.rand() * F.lit(8000)).cast(T.IntegerType())) \
    .withColumn("gear", (F.rand() * F.lit(7)).cast(T.IntegerType()))





producer = KafkaProducer(
    bootstrap_servers=brokers,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )

try :

    logger.info(f"Starting data generation for Kafka topic: {topic}")

    while True :
        
        for row in cars_df.toLocalIterator(): 
            producer.send(topic, value=row.asDict())
            logger.info(f"Sent event: {row.asDict()}")
            time.sleep(1)
            
except Exception as e:
    
    print(f"Kafka error : {e}")

finally:
    producer.close()
    spark.stop()
    logger.info("Kafka producer closed and Spark session stopped")
 