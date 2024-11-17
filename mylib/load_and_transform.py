from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Initialize Spark session
spark = (
    SparkSession.builder.appName("Transform Data")
    .config("spark.sql.catalogImplementation", "hive")
    .enableHiveSupport()
    .getOrCreate()
)


# Load and transform data
def load_and_transform() -> None:
    raw_table = "ids706_data_engineering.default.grad_students_raw"
    transformed_table = "ids706_data_engineering.default.grad_students_transformed"

    # Load raw data from Delta table
    df = spark.read.format("delta").table(raw_table)

    # Transform data
    transformed_df = df.withColumn(
        "Employment_rate", F.col("Grad_employed") / F.col("Grad_total")
    ).withColumn(
        "Major_category_size",
        F.when(F.col("Grad_total") > 10000, "Large").otherwise("Small"),
    )
    print("Data transformed successfully.")

    # Save transformed data
    transformed_df.write.format("delta").mode("overwrite").saveAsTable(
        transformed_table
    )
    print(f"Transformed data saved as Delta table: {transformed_table}")


# Execute transformation
load_and_transform()
