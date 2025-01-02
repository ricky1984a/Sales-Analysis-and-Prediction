import pandas as pd

# Sample dataset
data = {
    'customer_id': [101, 102, 101, 103],
    'order_id': [1, 2, 3, 4],
    'product_id': ['A', 'B', 'A', 'C'],
    'price': [50, 30, 50, 20],
    'quantity': [2, 1, 1, 5],
    'order_date': ['2024-01-01', '2024-01-02', '2024-02-01', '2024-03-01']
}

df = pd.DataFrame(data)

# Calculate Revenue
df['revenue'] = df['price'] * df['quantity']

# Revenue Analysis
total_revenue = df['revenue'].sum()
average_revenue_per_customer = df.groupby('customer_id')['revenue'].sum().mean()

# Customer Lifetime Value (CLV) Calculation
# 1. Average Purchase Value
purchase_value = df.groupby('customer_id')['revenue'].sum()
average_purchase_value = purchase_value.mean()

# 2. Average Purchase Frequency Rate
purchase_frequency = df.groupby('customer_id')['order_id'].count()
average_purchase_frequency = purchase_frequency.mean()

# 3. Customer Value
customer_value = average_purchase_value * average_purchase_frequency

# 4. Average Customer Lifespan (in months, assuming it's provided or calculated separately)
average_customer_lifespan = 12  # Example: 12 months

# CLV
clv = customer_value * average_customer_lifespan

# Results
print("Total Revenue:", total_revenue)
print("Average Revenue Per Customer:", average_revenue_per_customer)
print("Customer Lifetime Value (CLV):", clv)

# Revenue by Segment (e.g., Product)
revenue_by_product = df.groupby('product_id')['revenue'].sum()
print("Revenue by Product:\n", revenue_by_product)

# Revenue by Customer
revenue_by_customer = df.groupby('customer_id')['revenue'].sum()
print("Revenue by Customer:\n", revenue_by_customer)
