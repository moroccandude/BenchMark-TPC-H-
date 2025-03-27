#!/bin/bash

# Create jars directory if it doesn't exist
cd /jars

# Delta Lake Core
wget https://repo1.maven.org/maven2/io/delta/delta-core_2.12/2.4.0/delta-core_2.12-2.4.0.jar -O jars/delta-core_2.12-2.4.0.jar

# Delta Lake Spark Extension
wget https://repo1.maven.org/maven2/io/delta/delta-spark_2.12/2.4.0/delta-spark_2.12-2.4.0.jar -O jars/delta-spark_2.12-2.4.0.jar

# Optional: Iceberg if needed
wget https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.4.2/iceberg-spark-runtime-3.5_2.12-1.4.2.jar -O jars/iceberg-spark-runtime-3.5_2.12-1.4.2.jar

echo "Delta Lake and Iceberg jars downloaded successfully!"