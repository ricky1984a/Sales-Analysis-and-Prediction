from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, col

import os
import shutil

# Set Hadoop home and add to PATH (ensure winutils.exe is present in the directory)
os.environ["HADOOP_HOME"] = "C:\\hadoop"  # Replace with your Hadoop directory
os.environ["PATH"] += os.pathsep + os.path.join(os.environ["HADOOP_HOME"], "bin")

# Create a Spark session with configurations to handle Hadoop-related issues
spark = SparkSession.builder \
    .master("local[*]") \
    .appName("Product Analysis Report") \
    .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem") \
    .config("spark.hadoop.fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem") \
    .config("spark.hadoop.io.native.lib.available", False) \
    .getOrCreate()

# Path to the CSV file
file_path = "../output/top_products/part-00000-254a8dc6-24fd-4577-a543-2c8ecf1c8ec2-c000.csv"

# Read the CSV file into a DataFrame
raw_df = spark.read.csv(file_path, header=True, inferSchema=True)

raw_df.head()
raw_df.show()