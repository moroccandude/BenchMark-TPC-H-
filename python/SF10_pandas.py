import pandas as pd
import findspark
findspark.init()

from pyspark.sql import SparkSession
import os
import shutil
import time
import logging
import psutil
import json
from datetime import datetime
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# Configure logging
logging.basicConfig(
    format="%(asctime)s - %(levelname)s - %(message)s",
    level=logging.INFO,
    filename='benchmark_metrics.log'
)
logger = logging.getLogger(__name__)

def clear_spark_cache():
    """
    Clear various caches and temporary files to ensure clean benchmark
    """
    try:
        # Remove Spark event logs directory
        spark_events_dir = "/tmp/spark-events"
        if os.path.exists(spark_events_dir):
            shutil.rmtree(spark_events_dir)
        os.makedirs(spark_events_dir, exist_ok=True)

        # Clear system temporary directory
        temp_dir = "/tmp"
        for filename in os.listdir(temp_dir):
            file_path = os.path.join(temp_dir, filename)
            try:
                if filename.startswith("spark-") or filename.startswith("tmp"):
                    if os.path.isfile(file_path) or os.path.islink(file_path):
                        os.unlink(file_path)
                    elif os.path.isdir(file_path):
                        shutil.rmtree(file_path)
            except Exception as e:
                logger.warning(f"Could not remove {file_path}: {e}")

        # Attempt to clear Python's garbage collector
        import gc
        gc.collect()

        logger.info("Spark cache and temporary files cleared successfully")
    except Exception as e:
        logger.error(f"Error clearing Spark cache: {e}")

class BenchmarkMetrics:
    """
    Comprehensive metrics collection class for Spark benchmark
    """
    def __init__(self, session):
        """
        Initialize metrics collector

        Args:
            session (SparkSession): Active Spark session
        """
        self.spark_session = session
        self.metrics = {
            'timestamp': datetime.now().isoformat(),
            'formats': {}
        }

    def capture_system_metrics(self):
        """
        Capture system-wide performance metrics

        Returns:
            dict: System performance metrics
        """
        # CPU and Memory Metrics
        cpu_percent = psutil.cpu_percent(interval=1)
        memory = psutil.virtual_memory()

        system_metrics = {
            'cpu_usage_percent': cpu_percent,
            'memory_total_gb': round(memory.total / (1024**3), 2),
            'memory_used_gb': round(memory.used / (1024**3), 2),
            'memory_used_percent': memory.percent,
            'available_memory_gb': round(memory.available / (1024**3), 2)
        }

        return system_metrics

    def capture_spark_metrics(self):
        """
        Capture Spark-specific performance metrics

        Returns:
            dict: Spark performance metrics
        """
        try:
            spark_metrics = {
                'active_executors': self.spark_session.sparkContext.getConf().get('spark.executor.instances', 'N/A'),
                'default_parallelism': self.spark_session.sparkContext.defaultParallelism,
                'total_cores': self.spark_session.sparkContext.defaultParallelism,
                'spark_version': self.spark_session.version
            }

            # Attempt to get more detailed Spark metrics
            sc = self.spark_session.sparkContext

            # Storage Memory metrics
            try:
                storage_status = sc.getStorageMemoryStatus()
                spark_metrics['storage_memory_used_mb'] = storage_status[0][1]
            except:
                spark_metrics['storage_memory_used_mb'] = 'N/A'

            return spark_metrics
        except Exception as e:
            logger.error(f"Failed to capture Spark metrics: {e}")
            return {}

    def analyze_data_format(self, df, format_name, file_path, read_time):
        """
        Analyze performance metrics for a specific data format

        Args:
            df (DataFrame): Spark DataFrame
            format_name (str): Data format name
            file_path (str): Path to the data file
            read_time (float): Time taken to read the file

        Returns:
            dict: Performance metrics for the format
        """
        try:
            # Get file size
            file_size_mb = os.path.getsize(file_path) / (1024 * 1024)

            # Query plan and optimization details
            explain_plan = df._jdf.queryExecution().toString()

            format_metrics = {
                'read_time_seconds': read_time,
                'row_count': df.count(),
                'file_size_mb': round(file_size_mb, 2),
                'data_size_per_second_mb': round(file_size_mb / read_time, 2) if read_time > 0 else 0,
                'query_plan_length': len(explain_plan)
            }

            return format_metrics
        except Exception as e:
            logger.error(f"Failed to analyze {format_name} format: {e}")
            return {}

def create_session(master_path="local[*]", app_name="DataReader"):
    """
    Create and configure Spark session with performance optimizations

    Args:
        master_path (str): Spark master URL
        app_name (str): Application name

    Returns:
        SparkSession: Configured Spark session
    """
    try:
        # Clear existing caches before creating session
        clear_spark_cache()

        # Create temporary directories if they don't exist
        os.makedirs("/tmp/spark-events", exist_ok=True)

        session = SparkSession.builder \
            .appName(app_name) \
            .master(master_path) \
            .config("spark.sql.files.maxPartitionBytes", "128MB") \
            .config("spark.sql.shuffle.partitions", "200") \
            .config("spark.driver.memory", "8g") \
            .config("spark.executor.memory", "8g") \
            .config("spark.memory.fraction", "0.8") \
            .config("spark.memory.storageFraction", "0.5") \
            .config("spark.eventLog.enabled", "true") \
            .config("spark.eventLog.dir", "/tmp/spark-events") \
            .config("spark.executor.logs.rolling.strategy", "time") \
            .config("spark.executor.logs.rolling.time.interval", "daily") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.skewJoin.enabled", "true") \
            .config("spark.dynamicAllocation.enabled", "true") \
            .config("spark.shuffle.compress", "true") \
            .config("spark.rdd.compress", "true") \
            .config("spark.broadcast.compress", "true") \
            .config("spark.io.compression.codec", "snappy") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .getOrCreate()

        logger.info(f"Spark session created successfully: {app_name}")
        return session
    except Exception as e:
        logger.error(f"Failed to create Spark session: {e}")
        raise

# Define schema for customer data
CUSTOMER_SCHEMA = StructType([
    StructField("Customer_ID", IntegerType(), True),
    StructField("Customer_Number", StringType(), True),
    StructField("Hashed_Value", StringType(), True),
    StructField("Order_Count", IntegerType(), True),
    StructField("Phone_Number", StringType(), True),
    StructField("Total_Spent", DoubleType(), True),
    StructField("Category", StringType(), True),
    StructField("Notes", StringType(), True)
])

def read_data_formats(session, parent_path):
    """
    Read data in multiple formats and capture comprehensive performance metrics

    Args:
        session (SparkSession): Active Spark session
        parent_path (str): Base path for data files

    Returns:
        BenchmarkMetrics: Collected performance metrics
    """
    # Initialize metrics collector
    benchmark_metrics = BenchmarkMetrics(session)

    # Capture initial system and Spark metrics
    benchmark_metrics.metrics['system_metrics'] = benchmark_metrics.capture_system_metrics()
    benchmark_metrics.metrics['spark_metrics'] = benchmark_metrics.capture_spark_metrics()

    # Data format reading with detailed metrics
    data_formats = [
        ('csv', f"{parent_path}/csv/customer.csv"),
        ('orc', f"{parent_path}/orc/customer.orc"),
        ('parquet', f"{parent_path}/parquet/customer.parquet")
    ]

    for format_name, file_path in data_formats:
        try:
            start_time = time.time()

            # Read data based on format
            if format_name == 'csv':
                df = session.read.option("delimiter", ",").schema(CUSTOMER_SCHEMA).csv(file_path)
            elif format_name == 'orc':
                df = session.read.schema(CUSTOMER_SCHEMA).orc(file_path)
            elif format_name == 'parquet':
                df = session.read.schema(CUSTOMER_SCHEMA).parquet(file_path)

            read_time = time.time() - start_time

            # Analyze format performance
            format_metrics = benchmark_metrics.analyze_data_format(
                df, format_name, file_path, read_time
            )

            # Store metrics
            benchmark_metrics.metrics['formats'][format_name] = format_metrics

            # Create temp view for potential SQL analysis
            df.createOrReplaceTempView(f"customer_{format_name}")

        except Exception as e:
            logger.error(f"Failed to process {format_name} format: {e}")

    return benchmark_metrics

def save_metrics_report(metrics):
    """
    Save collected metrics to a JSON report

    Args:
        metrics (BenchmarkMetrics): Collected performance metrics
    """
    report_filename = f"benchmark_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"

    try:
        with open(report_filename, 'w') as f:
            json.dump(metrics.metrics, f, indent=4)

        logger.info(f"Metrics report saved: {report_filename}")
        print(f"\n--- Metrics Report Saved: {report_filename} ---")
    except Exception as e:
        logger.error(f"Failed to save metrics report: {e}")

def main():
    """
    Main execution function for TPC-H Benchmark
    """
    try:
        # Perform system-wide garbage collection and cache clearing
        import gc
        gc.collect()

        session = create_session(app_name="Advanced TPC-H Benchmark")

        # Capture comprehensive performance metrics
        benchmark_metrics = read_data_formats(
            session=session,
            parent_path="./CF10"
        )

        # Display and save metrics
        print("\n--- System Metrics ---")
        for key, value in benchmark_metrics.metrics['system_metrics'].items():
            print(f"{key}: {value}")

        print("\n--- Spark Metrics ---")
        for key, value in benchmark_metrics.metrics['spark_metrics'].items():
            print(f"{key}: {value}")

        print("\n--- Data Format Metrics ---")
        for format_name, metrics in benchmark_metrics.metrics['formats'].items():
            print(f"\n{format_name.upper()} Format:")
            for key, value in metrics.items():
                print(f"  {key}: {value}")

        # Save detailed metrics to JSON
        save_metrics_report(benchmark_metrics)

    except Exception as e:
        logger.error(f"Benchmark execution failed: {e}")
    finally:
        # Ensure proper cleanup
        if 'session' in locals():
            session.stop()

        # Clear caches after benchmark
        clear_spark_cache()

if __name__ == "__main__":
    main()