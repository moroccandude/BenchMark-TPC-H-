import os
import time
import logging
import json
from datetime import datetime
import psutil
import gc

try:
    import dask.dataframe as dd
    from dask.distributed import Client, LocalCluster
    DASK_AVAILABLE = True
except ImportError as e:
    DASK_AVAILABLE = False
    print(f"Import Error: {e}")
    print("Please install Dask with: pip install dask[complete]")

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    filename='dask_benchmark.log'
)
logger = logging.getLogger(__name__)

class DaskBenchmark:
    def __init__(self):
        self.client = None
        self.metrics = {
            'timestamp': datetime.now().isoformat(),
            'system': {},
            'dask': {},
            'formats': {}
        }

    def setup_cluster(self):
        """Initialize Dask cluster"""
        if not DASK_AVAILABLE:
            raise RuntimeError("Dask not available")

        try:
            self.client = Client(
                n_workers=4,
                threads_per_worker=2,
                memory_limit='8GB',
                silence_logs=logging.ERROR
            )
            logger.info("Dask cluster initialized")
            return True
        except Exception as e:
            logger.error(f"Cluster setup failed: {e}")
            return False

    def capture_metrics(self):
        """Collect system and Dask metrics"""
        # System metrics
        self.metrics['system'] = {
            'cpu': psutil.cpu_percent(),
            'memory': dict(psutil.virtual_memory()._asdict()),
            'disk': dict(psutil.disk_usage('/')._asdict())
        }

        # Dask metrics
        if self.client:
            try:
                self.metrics['dask'] = {
                    'workers': len(self.client.cluster.workers),
                    'threads': sum(w['nthreads'] for w in self.client.cluster.workers.values()),
                    'memory': self.client.cluster.worker_spec[0]['options']['memory_limit'],
                    'version': dd.__version__
                }
            except Exception as e:
                logger.error(f"Failed to get Dask metrics: {e}")

    def benchmark_format(self, file_path, format_name):
        """Benchmark a single file format"""
        if not os.path.exists(file_path):
            logger.error(f"File not found: {file_path}")
            return None

        try:
            file_size = os.path.getsize(file_path) / (1024 ** 2)  # MB
            start_time = time.time()

            if format_name == 'csv':
                df = dd.read_csv(file_path,
                                 dtype={
                                     'Customer_ID': 'int64',
                                     'Customer_Number': 'object',
                                     'Hashed_Value': 'object',
                                     'Order_Count': 'int64',
                                     'Phone_Number': 'object',
                                     'Total_Spent': 'float64',
                                     'Category': 'object',
                                     'Notes': 'object'
                                 })
            elif format_name == 'parquet':
                df = dd.read_parquet(file_path, engine='pyarrow')
            else:
                raise ValueError(f"Unsupported format: {format_name}")

            # Force computation
            df = df.persist()
            row_count = df.shape[0].compute()
            elapsed = time.time() - start_time

            return {
                'file_size_mb': round(file_size, 2),
                'read_time_sec': round(elapsed, 2),
                'rows_per_sec': round(row_count / elapsed, 2),
                'mb_per_sec': round(file_size / elapsed, 2),
                'row_count': int(row_count)
            }

        except Exception as e:
            logger.error(f"Benchmark failed for {format_name}: {e}")
            return None

    def run_benchmarks(self, data_dir):
        """Run all benchmarks"""
        if not self.setup_cluster():
            return False

        self.capture_metrics()

        formats = [
            ('csv', os.path.join(data_dir, 'csv/customer.csv')),
            ('parquet', os.path.join(data_dir, 'parquet/customer.parquet'))
        ]

        for name, path in formats:
            result = self.benchmark_format(path, name)
            if result:
                self.metrics['formats'][name] = result
                logger.info(f"Completed {name} benchmark")

        return True

    def save_results(self):
        """Save benchmark results"""
        filename = f"dask_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        try:
            with open(filename, 'w') as f:
                json.dump(self.metrics, f, indent=2)
            logger.info(f"Results saved to {filename}")
            print(f"\nResults saved to: {filename}")
        except Exception as e:
            logger.error(f"Failed to save results: {e}")

    def cleanup(self):
        """Clean up resources"""
        if self.client:
            self.client.close()
            logger.info("Dask cluster shutdown")

def main():
    print("Starting Dask Benchmark...")
    benchmark = DaskBenchmark()

    try:
        # Update this path to your data directory
        data_path = "./CF10"

        if benchmark.run_benchmarks(data_path):
            print("\nBenchmark Results:")
            print(f"System CPU: {benchmark.metrics['system']['cpu']}%")
            print(f"Memory Used: {benchmark.metrics['system']['memory']['used']/1e9:.2f} GB")

            print("\nData Formats:")
            for fmt, results in benchmark.metrics['formats'].items():
                print(f"\n{fmt.upper()}:")
                for k, v in results.items():
                    print(f"  {k}: {v}")

            benchmark.save_results()
    except Exception as e:
        print(f"Error during benchmark: {e}")
    finally:
        benchmark.cleanup()
        print("\nBenchmark completed")

if __name__ == "__main__":
    main()