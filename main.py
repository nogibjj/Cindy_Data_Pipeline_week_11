"""
Main CLI or app entry point
"""

from mylib.extract import extract_spark
# from mylib.load_and_transform import load_and_transform
# from mylib.sql_query import query_delta_table


def main():
    """
    Unified main function to execute the pipeline steps sequentially.
    """
    print("Starting data extraction...")
    extract_spark()
    print("Data extraction completed.\n")

    # print("Starting data transformation...")
    # load_and_transform()
    # print("Data transformation completed.\n")

    # print("Starting data query...")
    # query_delta_table()
    # print("Data query completed.")


# Run the pipeline
if __name__ == "__main__":
    main()

