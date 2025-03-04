from pyspark.sql import SparkSession
from pyspark.sql import types as T
import logging



# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# Initialize Spark session'
spark = SparkSession\
    .builder\
    .master("local")\
    .appName('ModelCreation')\
    .getOrCreate()


logger.info("Spark Session initialized")

# Define explicit schema
schema = T.StructType([
    T.StructField("color_id", T.IntegerType(), False),
    T.StructField("color_name", T.StringType(), False)
])

# Data
car_colors = [
    (1, 'Black'),
    (2, 'Red'),
    (3, 'Gray'),
    (4, 'White'),
    (5, 'Green'),
    (6, 'Blue'),
    (7, 'Pink')
]


# Create DataFrame with schema
car_colors_df = spark.createDataFrame(car_colors, schema=schema)

car_colors_df.show()


# Write data to Parquet format with error handling
try:
    car_colors_df.write.parquet('s3a://spark/data/dims/car_colors', mode='overwrite')
    logger.info("Data successfully written to 's3a://spark/data/dims/car_colors'")
except Exception as e:
    logger.error(f"Error writing data: {e}")


# Stop Spark session
spark.stop()
logger.info("Spark Session stopped")