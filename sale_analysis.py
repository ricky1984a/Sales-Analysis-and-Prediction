from pyspark.sql import SparkSession

# Create a new Spark session
spark = SparkSession.builder.appName("Sales_Analysis").getOrCreate()

# Sales Data
sales_data = [
    ("2024-01-15", 1, "Product_A", 10, 1001),
    ("2024-01-15", 2, "Product_B", 15, 1002),
    ("2024-02-11", 3, "Product_C", 20, 1003),
    ("2024-02-17", 4, "Product_D", 5, 1004),
    ("2024-04-15", 1, "Product_A", 8, 1001),
    ("2024-05-16", 2, "Product_B", 12, 1002),
    ("2024-06-17", 3, "Product_C", 6, 1003),
    ("2024-07-18", 4, "Product_D", 13, 1004),
]

sales_columns = ["order_date", "product_id", "product_name", "quantity_sold", "customer_id"]
sales_df = spark.createDataFrame(sales_data, sales_columns)

# Register Sales DataFrame as a Temp View
sales_df.createOrReplaceTempView("sales")

# Query Top Products using Spark SQL
top_products = spark.sql("""
    SELECT product_name, SUM(quantity_sold) AS total_quantity
    FROM sales
    GROUP BY product_name
    ORDER BY total_quantity DESC
    LIMIT 10
""")
print("Top Products:")
top_products.show()

# Query Seasonal Trends using Spark SQL
seasonal_trends = spark.sql("""
    SELECT MONTH(order_date) AS month, SUM(quantity_sold) AS total_sales
    FROM sales
    GROUP BY month
    ORDER BY month
""")
print("Seasonal Trends:")
seasonal_trends.show()

# Query Frequent Buyers using Spark SQL
frequent_buyers = spark.sql("""
    SELECT customer_id, COUNT(order_date) AS purchase_count
    FROM sales
    GROUP BY customer_id
    ORDER BY purchase_count DESC
    LIMIT 10
""")
print("Frequent Buyers:")
frequent_buyers.show()

# Stop the Spark session
spark.stop()
