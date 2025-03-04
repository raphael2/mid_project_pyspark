from pyspark.sql import SparkSession
from pyspark.sql import types as T
import logging




# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Initialize Spark 
spark = SparkSession.builder\
    .master("local")\
    .appName("ModelCreation")\
    .getOrCreate()

logger.info("Spark Session initialized")

# Define explicit schema
schema = T.StructType([
    T.StructField("model_id", T.IntegerType(), False),
    T.StructField("car_brand", T.StringType(), False),
    T.StructField("car_model", T.StringType(), False)
])

# Data
car_models = [
    (1, 'Mazda', '3'),
    (2, 'Mazda', '6'),
    (3, 'Toyota', 'Corolla'),
    (4, 'Hyundai', 'i20'),
    (5, 'Kia', 'Sportage'),
    (6, 'Kia', 'Rio'),
    (7, 'Kia', 'Picanto')
]

# Create DataFrame with schema
car_models_df = spark.createDataFrame(car_models, schema=schema)


# Write data to Parquet format with error handling
try:
    car_models_df.write.parquet('s3a://spark/data/dims/car_models', mode='overwrite')
    logger.info(f"Data successfully written to 's3a://spark/data/dims/car_models'")
except Exception as e:
    logger.error(f"Error writing data: {e}")

# Stop Spark session
spark.stop()
logger.info("Spark Session stopped")
