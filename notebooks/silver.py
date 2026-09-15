# Databricks notebook source
customers_bronze = spark.read.table("bronze_customers")
orders_bronze = spark.read.table("bronze_orders")
products_bronze = spark.read.table("bronze_products")

# COMMAND ----------

customers_bronze.show(5)
customers_bronze.printSchema()

# COMMAND ----------

products_bronze.show(5)
products_bronze.printSchema()

# COMMAND ----------

orders_bronze.show(5)
products_bronze.show(5)

# COMMAND ----------

from pyspark.sql.functions import col

customers_silver = customers_bronze \
    .dropDuplicates() \
    .dropna(subset=["customer_id"]) \
    .withColumn("customer_id", col("customer_id").cast("string"))

# COMMAND ----------

products_silver = products_bronze \
    .dropDuplicates() \
    .dropna(subset=["product_id"]) \
    .withColumn("unit_price", col("unit_price").cast("double"))

# COMMAND ----------

orders_silver = orders_bronze \
    .dropDuplicates() \
    .dropna(subset=["order_id", "customer_id"]) \
    .withColumn("quantity", col("quantity").cast("int"))

# COMMAND ----------

customers_silver.write.mode("overwrite").saveAsTable("silver_customers")
orders_silver.write.mode("overwrite").saveAsTable("silver_orders")
products_silver.write.mode("overwrite").saveAsTable("silver_products")

# COMMAND ----------

from pyspark.sql.functions import to_date, col

customers_silver = customers_silver.withColumn(
    "signup_date",
    to_date(col("signup_date"), "yyyy-MM-dd")
)

# COMMAND ----------

orders_silver = orders_silver.withColumn(
    "order_date",
    to_date(col("order_date"), "yyyy-MM-dd")
)

# COMMAND ----------

spark.sql("SELECT * FROM silver_customers LIMIT 5").show()