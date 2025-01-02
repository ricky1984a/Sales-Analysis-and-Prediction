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
    .appName("Sales Analysis Report") \
    .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem") \
    .config("spark.hadoop.fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem") \
    .config("spark.hadoop.io.native.lib.available", False) \
    .getOrCreate()

# Path to the CSV file
file_path = "./sales_data/06-12-2020-10-20-33.csv"

# Read the CSV file into a DataFrame
raw_df = spark.read.csv(file_path, header=True, inferSchema=True)

# Rename and map columns based on the assumed schema
renamed_df = raw_df.selectExpr(
    "TIME_ID as order_date",
    "PROD_ID as product_id",
    "CHANNEL_ID as product_name",  # Placeholder for missing product_name
    "QUANTITY_SOLD as quantity_sold",
    "CUST_ID as customer_id"
)

# Convert `order_date` to proper date format
renamed_df = renamed_df.withColumn(
    "order_date",
    to_date("order_date", "dd-MM-yy")  # Match your data format
)

# Validate data conversion (check for nulls in `order_date`)
renamed_df.filter(col("order_date").isNull()).show()

# Re-register the DataFrame with corrected dates
renamed_df.createOrReplaceTempView("sales")

# Query Top Products using Spark SQL
top_products = spark.sql("""
    SELECT product_id, SUM(quantity_sold) AS total_quantity
    FROM sales
    GROUP BY product_id
    ORDER BY total_quantity DESC
    LIMIT 10
""")
print("Top Products:")
top_products.show()

# Query Seasonal Trends using Spark SQL
seasonal_trends = spark.sql("""
    SELECT MONTH(order_date) AS month, SUM(quantity_sold) AS total_sales
    FROM sales
    WHERE order_date IS NOT NULL
    GROUP BY month
    ORDER BY month
""")
print("Seasonal Trends:")
seasonal_trends.show()

# Query Frequent Buyers using Spark SQL
frequent_buyers = spark.sql("""
    SELECT customer_id, COUNT(order_date) AS purchase_count
    FROM sales
    WHERE order_date IS NOT NULL
    GROUP BY customer_id
    ORDER BY purchase_count DESC
    LIMIT 10
""")
print("Frequent Buyers:")
frequent_buyers.show()

# Define the output directory
output_path = "../output/"

# Clean the output directory if it exists
if os.path.exists(output_path):
    shutil.rmtree(output_path)

# Export Results to CSV
top_products.write.csv(output_path + "top_products", mode="overwrite", header=True)
seasonal_trends.write.csv(output_path + "seasonal_trends", mode="overwrite", header=True)
frequent_buyers.write.csv(output_path + "frequent_buyers", mode="overwrite", header=True)

# Stop the Spark session
spark.stop()
