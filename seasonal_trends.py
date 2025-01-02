from pyspark.sql import SparkSession
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import os
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import confusion_matrix, classification_report
from sklearn.utils.class_weight import compute_class_weight
import numpy as np

# Set Hadoop home and add to PATH (ensure winutils.exe is present in the directory)
os.environ["HADOOP_HOME"] = "C:\\hadoop"
os.environ["PATH"] += os.pathsep + os.path.join(os.environ["HADOOP_HOME"], "bin")

# Create a Spark session
spark = SparkSession.builder \
    .master("local[*]") \
    .appName("Sales Analysis and Prediction") \
    .getOrCreate()

# Path to the CSV file
file_path = "../output/seasonal_trends/part-00000-4fab1ea6-4a57-4026-a92a-795ad2fd93fe-c000.csv"

# Read the CSV file into a PySpark DataFrame
raw_data = spark.read.csv(file_path, header=True, inferSchema=True)
raw_data = raw_data.toDF("month", "total_sales")  # Rename columns explicitly

# Inspect PySpark DataFrame
print("PySpark DataFrame Schema:")
raw_data.printSchema()

# Convert PySpark DataFrame to Pandas DataFrame
raw_data_pd = raw_data.toPandas()

# Data cleaning
raw_data_pd['total_sales'] = pd.to_numeric(raw_data_pd['total_sales'], errors='coerce')
raw_data_pd['month'] = raw_data_pd['month'].astype(str)
raw_data_pd = raw_data_pd.dropna()

if raw_data_pd.empty:
    raise ValueError("The DataFrame is empty after cleaning.")

# Check class balance
print("Class Distribution in Target Variable:")
print(raw_data_pd['total_sales'].value_counts())

# Encode 'month' column
raw_data_pd['month_encoded'] = pd.factorize(raw_data_pd['month'])[0]

# Optional: Bin the target variable (e.g., low, medium, high sales)
raw_data_pd['sales_category'] = pd.cut(
    raw_data_pd['total_sales'],
    bins=3,
    labels=["Low", "Medium", "High"]
)

# Features and target
X = raw_data_pd[['month_encoded']]
y = raw_data_pd['sales_category']

# Split dataset into training and testing sets
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42, stratify=y)

# Handle class imbalance using class weights
class_weights = compute_class_weight(
    class_weight='balanced',
    classes=np.unique(y_train),
    y=y_train
)
class_weights_dict = dict(zip(np.unique(y_train), class_weights))
print("Class Weights:", class_weights_dict)

# Random Forest Classifier
model = RandomForestClassifier(class_weight=class_weights_dict, random_state=42)
model.fit(X_train, y_train)
y_pred = model.predict(X_test)

# Model Evaluation
print("Confusion Matrix:")
print(confusion_matrix(y_test, y_pred))

print("\nClassification Report:")
print(classification_report(y_test, y_pred, zero_division=1))

# Visualization
plt.figure(figsize=(10, 6))
sns.scatterplot(data=raw_data_pd, x='month', y='total_sales', marker='o')
plt.title("Monthly Total Sales")
plt.xlabel("Month")
plt.ylabel("Total Sales")
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()
