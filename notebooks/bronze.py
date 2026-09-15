# Databricks notebook source
# Import required libraries
import pandas as pd
import requests
from io import StringIO

# COMMAND ----------

# Function to load CSV from GitHub
def load_csv_from_github(url):
    response = requests.get(url)
    data = StringIO(response.text)
    return pd.read_csv(data)

# COMMAND ----------

# Load CSV files from GitHub
customers_url = "https://raw.githubusercontent.com/KetroSithole/kasi-mart-data-engineering-project1/main/customers.csv"
orders_url = "https://raw.githubusercontent.com/KetroSithole/kasi-mart-data-engineering-project1/main/orders.csv"
products_url = "https://raw.githubusercontent.com/KetroSithole/kasi-mart-data-engineering-project1/main/products.csv"

customers_pd = load_csv_from_github(customers_url)
orders_pd = load_csv_from_github(orders_url)
products_pd = load_csv_from_github(products_url)

# COMMAND ----------

# Create Spark DataFrames
customers_df = spark.createDataFrame(customers_pd)
orders_df = spark.createDataFrame(orders_pd)
products_df = spark.createDataFrame(products_pd)

# COMMAND ----------

# Display DataFrames
customers_df.show(5)
orders_df.show(5)
products_df.show(5)

# COMMAND ----------

# Write DataFrames to tables
customers_df.write.mode("overwrite").saveAsTable("bronze_customers")
orders_df.write.mode("overwrite").saveAsTable("bronze_orders")
products_df.write.mode("overwrite").saveAsTable("bronze_products")