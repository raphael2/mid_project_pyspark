from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import logging


# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# Constants
million = 1000000
num_cars = 20  # Number of cars to generate


# Initialize Spark session
spark = SparkSession\
    .builder\
    .master("local")\
    .appName("CarsGenerator")\
    .getOrCreate()
logger.info("Spark Session initialized")



temp_cars_df = spark.range(num_cars)



cars_gen_df = temp_cars_df.select(
    (F.monotonically_increasing_id() + million).cast("int").alias("car_id"), 
    ((F.rand()*900*million + 100*million).cast("int")).alias("driver_id"),
    ((F.rand()*7 + 1).cast("int")).alias("model_id"),
    ((F.rand()*7 + 1).cast("int")).alias("color_id")
)

cars_gen_df.show()


# Write data to Parquet format with error handling
try:
    cars_gen_df.write.parquet('s3a://spark/data/dims/cars', mode='overwrite')
    logger.info("Data successfully written to s3a://spark/data/dims/cars")
except Exception as e:
    logger.error(f"Error writing data: {e}")

# Stop Spark session
spark.stop()
logger.info("Spark Session stopped")


spark.stop()
