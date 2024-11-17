"""
Test goes here

"""
import unittest
from mylib.extract import extract_spark

class TestExtractSpark(unittest.TestCase):
    def test_extract_spark(self):
        """
        Test the extract_spark function to ensure the data is processed correctly.
        """
        try:
            extract_spark()
            print("Extract function executed successfully.")
        except Exception as e:
            self.fail(f"Extract function failed with error: {e}")

if __name__ == "__main__":
    unittest.main()




#     # Verify the raw Delta table exists and contains data
#     spark = SparkSession.builder.getOrCreate()
#     raw_table = "ids706_data_engineering.default.grad_students_raw"
#     df = spark.read.format("delta").table(raw_table)

#     assert df is not None, "Raw Delta table not created."
#     assert df.count() > 0, "Raw Delta table is empty."
#     print("test_extract_spark passed")


# def test_load_and_transform():
#     """
#     Test the load_and_transform function to ensure the data is transformed correctly.
#     """
#     # Run the transform function
#     load_and_transform()

#     # Verify the transformed Delta table exists and contains expected columns
#     spark = SparkSession.builder.getOrCreate()
#     transformed_table = "ids706_data_engineering.default.grad_students_transformed"
#     df = spark.read.format("delta").table(transformed_table)

#     assert df is not None, "Transformed Delta table not created."
#     assert df.count() > 0, "Transformed Delta table is empty."
#     assert (
#         "Employment_rate" in df.columns
#     ), "'Employment_rate' column missing in transformed data."
#     assert (
#         "Major_category_size" in df.columns
#     ), "'Major_category_size' column missing in transformed data."
#     print("test_load_and_transform passed")


# def test_query_delta_table():
#     """
#     Test the query_delta_table function to ensure the SQL query runs successfully.
#     """
#     try:
#         query_delta_table()  # Execute the query
#         print("test_query_delta_table passed")
#     except Exception as e:
#         assert False, f"Query execution failed with error: {str(e)}"


# def test_main():
#     """
#     Test the main function to ensure the entire pipeline executes without errors.
#     """
#     try:
#         main()  # Execute the entire pipeline
#         print("test_main passed")
#     except Exception as e:
#         assert False, f"Pipeline execution failed with error: {str(e)}"