import os
from typing import Union, Optional
import findspark
import  pandas as pd

findspark.init()

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType


class DataReader:
    def __init__(self, app_name: str = "DataReader"):
        """
        Initialize Spark Session

        Args:
            app_name (str): Name of the Spark application
        """
        self.spark = SparkSession.builder \
            .appName(app_name) \
            .master("spark://localhost:7077") \
            .config("spark.sql.files.maxPartitionBytes", "128MB") \
            .getOrCreate()

    def read_csv(
            self,
            path: str,
            header: bool = True,
            delimiter: str = ",",
            schema: Optional[StructType] = None
    ) -> DataFrame:
        """
        Read CSV file with optional schema

        Args:
            path (str): Path to CSV file
            header (bool): Whether CSV has header row
            delimiter (str): CSV delimiter
            schema (StructType): Optional predefined schema

        Returns:
            DataFrame: Spark DataFrame
        """
        try:
            reader = self.spark.read \
                .option("header", header) \
                .option("delimiter", delimiter) \
                .option("inferSchema", "true")

            df = reader.csv(path)

            print(f"CSV Read from {path}")
            print("Schema:")
            df.printSchema()

            return df

        except Exception as e:
            print(f"Error reading CSV: {e}")
            return None

    def read_parquet(self, path: str) -> DataFrame:
        """
        Read Parquet file

        Args:
            path (str): Path to Parquet file

        Returns:
            DataFrame: Spark DataFrame
        """
        try:
            df = self.spark.read.parquet(path)

            print(f"Parquet Read from {path}")
            print("Schema:")
            df.printSchema()

            return df

        except Exception as e:
            print(f"Error reading Parquet: {e}")
            return None

    def read_orc(
            self,
            path: str,
            multiline: bool = False,

    ) -> DataFrame:
        """
        Read JSON file

        Args:
            path (str): Path to JSON file
            multiline (bool): Whether JSON is multiline
            schema (StructType): Optional predefined schema

        Returns:
            DataFrame: Spark DataFrame
        """
        try:
            reader = self.spark.read \
                .option("multiline", multiline)

            df = reader.orc(path)

            print(f"JSON Read from {path}")
            print("Schema:")
            df.printSchema()

            return df

        except Exception as e:
            print(f"Error reading JSON: {e}")
            return None


    def preview_data(self, df: DataFrame, rows: int = 10):
        """
        Preview DataFrame

        Args:
            df (DataFrame): Spark DataFrame
            rows (int): Number of rows to show
        """
        if df is not None:
            print("\nData Preview:")
            df.show(rows)

    def close(self):
        """
        Close Spark Session
        """
        self.spark.stop()


def main():
    # Example usage
    reader = DataReader("DataReaderDemo")
    parent_path="./data_CF10"
    # Read CSV
    csv_df = reader.read_csv(
        path=f"{parent_path}/csv/customer.csv/",

    )
    reader.preview_data(csv_df)

    # Read Parquet
    parquet_df = reader.read_parquet(
        path=f"{parent_path}/parquet/customer.parquet/"
    )
    reader.preview_data(parquet_df)
    #
    df = reader.spark.read.parquet(f"{parent_path}/parquet/customer.parquet")
    print(df.limit(10))
    # print(f"Parquet Read from {path}")
    print("Schema:")
    df.printSchema()
    # Example JSON Reading
    orc_df = reader.read_orc(
        path=f"{parent_path}/orc/customer.orc"
    )
    reader.preview_data(orc_df)

    # Close Spark Session
    reader.close()


if __name__ == "__main__":
    main()
