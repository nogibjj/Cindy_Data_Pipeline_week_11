from pyspark.sql import SparkSession

# Initialize Spark session
spark = (
    SparkSession.builder.appName("Query Data")
    .config("spark.sql.catalogImplementation", "hive")
    .enableHiveSupport()
    .getOrCreate()
)


# Query Delta table
def query_delta_table() -> None:
    transformed_table = "ids706_data_engineering.default.grad_students_transformed"

    query = f"""
    SELECT 
        Major_code,
        Major,
        Major_category,
        Grad_total,
        Grad_employed,
        Grad_unemployed,
        Grad_unemployment_rate,
        Grad_share,
        Grad_premium,
        Employment_rate,
        Major_category_size
    FROM 
        {transformed_table}
    WHERE 
        Employment_rate IS NOT NULL
    ORDER BY 
        Employment_rate DESC
    LIMIT 10
    """
    result = spark.sql(query)
    result.show()
    print("SQL query executed successfully.")


# Execute query
query_delta_table()
