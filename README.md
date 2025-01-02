# Sales Analysis and Prediction

This project analyzes sales data and predicts future trends using a Random Forest classifier. The workflow includes data cleaning, visualization, and machine learning. It utilizes PySpark for data processing and Python libraries for analysis and visualization.

---

## Features

1. **Data Ingestion**: Load sales data from a CSV file using PySpark.
2. **Data Cleaning**: Handle missing values and ensure proper data types.
3. **Visualization**: Scatter plot of monthly sales trends.
4. **Machine Learning**: Train a Random Forest classifier to predict sales trends.
5. **Evaluation**: Confusion matrix and classification report to assess model performance.

---

## Prerequisites

- Python 3.x
- PySpark
- pandas
- seaborn
- matplotlib
- scikit-learn
- Hadoop setup with `winutils.exe` for Windows users

---

## Setup

1. **Install Required Packages**:
   ```bash
   pip install pyspark pandas matplotlib seaborn scikit-learn
   ```

2. **Configure Hadoop for PySpark** (Windows users):
   - Download Hadoop binaries from [Hadoop Releases](https://hadoop.apache.org/releases.html).
   - Extract to a directory (e.g., `C:\hadoop`).
   - Ensure `winutils.exe` is present in the `bin` folder of the Hadoop directory.
   - Set environment variables:
     ```python
     os.environ["HADOOP_HOME"] = "C:\hadoop"
     os.environ["PATH"] += os.pathsep + os.path.join(os.environ["HADOOP_HOME"], "bin")
     ```

3. **Set Up CSV File**:
   - Place the sales data CSV file (`part-00000-*.csv`) in the `output/seasonal_trends/` directory.
   - Ensure the file contains columns for `month` and `total_sales`.

---

## Usage

1. **Run the Script**:
   ```bash
   python sales_analysis.py
   ```

2. **Output**:
   - DataFrame details before and after cleaning.
   - Confusion matrix and classification report for the model.
   - A scatter plot visualizing sales trends.

---

## Key Components

### Data Preparation
- **Input File**: A CSV file containing `month` and `total_sales` data.
- **Cleaning**: Ensures all values are numeric and non-null.

### Visualization
- Scatter plot for monthly sales trends.

### Machine Learning
- **Model**: Random Forest Classifier.
- **Training Data**: Encoded months and sales data.
- **Evaluation**: Metrics include precision, recall, and F1-score.

---

## Example Output

### Confusion Matrix
```
[[TP, FP],
 [FN, TN]]
```

### Classification Report
```
              precision    recall  f1-score   support
Class_1       0.85      0.88      0.87       20
Class_2       0.80      0.78      0.79       15
```

### Visualization
- Scatter plot displaying the sales trend over months.

---

## Notes

- Update the `file_path` variable to point to your CSV file.
- The script is tested on local machine configurations. Ensure PySpark and Hadoop are correctly set up in your environment.

---

## License

This project is licensed under the MIT License.

