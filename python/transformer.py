import os
from typing import Optional
import findspark
findspark.init()
from pyspark.sql import SparkSession, DataFrame
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
def create_spark_session(app_name: str = "DataConverter",
                         enable_iceberg: bool = False):
    logger=logging.getLogger(__name__)
    handler=logging.StreamHandler()
    logger.addHandler(handler)
    """
    Create a Spark session with optional format support

    Args:
        app_name (str): Name of the Spark application
        enable_iceberg (bool): Enable Apache Iceberg support

    Returns:
        SparkSession: Configured Spark session
    """
    # Prepare additional jars
    jars = []

    # Iceberg configuration
    if enable_iceberg:
        iceberg_jar_path = "./jars/iceberg-spark-runtime-3.5_2.12-1.4.2.jar"
        if not os.path.exists(iceberg_jar_path):
            print(f"Warning: Iceberg JAR not found at {iceberg_jar_path}")
        else:
            jars.append(iceberg_jar_path)

    # Build Spark session
    builder = SparkSession.builder \
        .master("local[*]") \
    .appName(app_name) \
        .config("spark.executor.memory", "2g") \
        .config("spark.cores.max", "4")

# Add jars if any
    if jars:
        builder = builder.config("spark.jars", ",".join(jars))

# Iceberg specific configurations
    if enable_iceberg:
       builder = builder \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog")

    return builder.getOrCreate()

def convert_to_format(
        spark: SparkSession,
        input_path: str,
        output_path: str,
        output_format: str = "parquet",
        delimiter: str = "|",
        header: bool = True,
        table_name: Optional[str] = None
) -> None:
    """
    Convert input file to multiple formats

    Supported formats:
    - parquet
    - orc
    - csv
    - iceberg
    """
    try:
        # Validate input file exists
        if not os.path.exists(input_path):
            raise FileNotFoundError(f"Input file not found: {input_path}")

        # Read input file
        df = spark.read \
            .option("delimiter", delimiter) \
            .option("header","true") \
            .option("inferSchema", "true") \
            .option("encoding", "UTF-8") \
            .csv(input_path,)
        print(f"columns : \n {df.columns}")
        # Preview data
        print(f"Preview of data from {input_path}:")
        df.show(5)

        # Create output directory if not exists
        if output_format.lower() != "iceberg":
            os.makedirs(os.path.dirname(output_path), exist_ok=True)

        # # Convert to different formats
        # format_handlers = {
        #     "parquet": lambda: df.write.mode("overwrite").parquet(output_path),
        #     "orc": lambda: df.write.mode("overwrite").orc(output_path),
        #     "csv": lambda: df.write.mode("overwrite").csv(output_path, header=header),
        #     # "iceberg": lambda: _handle_iceberg_write(spark, df, table_name)
        # }
        #
        # # Execute format conversion
        # if output_format.lower() in format_handlers:
        #     format_handlers[output_format.lower()]()
        #     print(f"Successfully converted to {output_format} at {output_path}")
        # else:
        #     raise ValueError(f"Unsupported output format: {output_format}")

    except Exception as e:
        print(f"Error during conversion: {str(e)}")
        raise

def _handle_iceberg_write(spark: SparkSession, df: DataFrame, table_name: str):
    """
    Special handler for Iceberg table writing
    """
    if not table_name:
        raise ValueError("table_name is required for Iceberg format")

    df.writeTo(f"local.{table_name}").createOrReplace()

def main():
    # Verify current working directory and input file
    print(f"Current path: {os.getcwd()}")
    input_file = "./generated_data/customer.tbl"

    # Check if input file exists
    if not os.path.exists(input_file):
        print(f"Input file not found: {input_file}")
        return

    # Ensure output directories exist
    os.makedirs("./CF10/parquet", exist_ok=True)
    os.makedirs("./CF10/orc", exist_ok=True)
    os.makedirs("./CF10/csv", exist_ok=True)
    # os.makedirs("./CF10/iceberg_warehouse", exist_ok=True)

    # Create Spark session
    session = create_spark_session("multi-format-converter")

    # Conversion scenarios
    conversion_scenarios = [
        {"format": "parquet", "path": "./CF10/parquet/customer.parquet"},
        {"format": "orc", "path": "./CF10/orc/customer.orc"},
        {"format": "csv", "path": "./CF10/csv/customer.csv"},
        # Uncomment if Iceberg is configured
        # {"format": "iceberg", "table_name": "customer_CF10"}
    ]

    # Perform conversions
    for scenario in conversion_scenarios:
        try:
            convert_to_format(
                spark=session,
                input_path=input_file,
                output_path=scenario.get("path", "/test_output"),
                output_format=scenario["format"],
                table_name=scenario.get("table_name")
            )
        except Exception as e:
            print(f"Conversion to {scenario['format']} failed: {e}")

    # Close Spark session
    session.stop()

if __name__ == "__main__":
    main()