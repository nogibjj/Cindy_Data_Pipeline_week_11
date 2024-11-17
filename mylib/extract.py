from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, 
    StructField, 
    StringType, 
    IntegerType, 
    FloatType
)
import requests

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Delta Lake Example") \
    .config("spark.jars.packages", 
            "io.delta:delta-core_2.12:2.4.0") \
    .config("spark.sql.extensions", 
            "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", 
            "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# Define schema
schema = StructType([
    StructField("Major_code", StringType(), True),
    StructField("Major", StringType(), True),
    StructField("Major_category", StringType(), True),
    StructField("Grad_total", IntegerType(), True),
    StructField("Grad_sample_size", IntegerType(), True),
    StructField("Grad_employed", IntegerType(), True),
    StructField("Grad_full_time_year_round", IntegerType(), True),
    StructField("Grad_unemployed", IntegerType(), True),
    StructField("Grad_unemployment_rate", FloatType(), True),
    StructField("Grad_median", IntegerType(), True),
    StructField("Grad_P25", IntegerType(), True),
    StructField("Grad_P75", IntegerType(), True),
    StructField("Nongrad_total", IntegerType(), True),
    StructField("Nongrad_employed", IntegerType(), True),
    StructField("Nongrad_full_time_year_round", IntegerType(), True),
    StructField("Nongrad_unemployed", IntegerType(), True),
    StructField("Nongrad_unemployment_rate", FloatType(), True),
    StructField("Nongrad_median", IntegerType(), True),
    StructField("Nongrad_P25", IntegerType(), True),
    StructField("Nongrad_P75", IntegerType(), True),
    StructField("Grad_share", FloatType(), True),
    StructField("Grad_premium", FloatType(), True)
])

def extract_spark() -> None:
    url = "https://raw.githubusercontent.com/fivethirtyeight/data/master/college-majors/grad-students.csv"
    file_path = "/tmp/grad-students.csv"

    # Download CSV file
    try:
        response = requests.get(url)
        response.raise_for_status()
        with open(file_path, "wb") as file:
            file.write(response.content)
        print(f"File downloaded to {file_path}")
    except requests.RequestException as e:
        print(f"Failed to download file: {e}")
        return

    # Load data into Spark DataFrame
    try:
        df = spark.read.option("header", "true").schema(schema).csv(file_path)
        print("Data loaded into Spark DataFrame")
    except Exception as e:
        print(f"Failed to load CSV into DataFrame: {e}")
        return

    # Save data as Parquet (to bypass Delta issues)
    try:
        parquet_path = "/tmp/parquet/grad_students_raw"
        df.write.format("parquet").mode("overwrite").save(parquet_path)
        print(f"Data saved as Parquet at {parquet_path}")
    except Exception as e:
        print(f"Failed to save Parquet table: {e}")

# Execute extraction
extract_spark()
