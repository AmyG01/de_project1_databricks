# Databricks notebook source
spark.sql("SELECT COUNT(*) AS customers_count FROM silver_customers").show()
spark.sql("SELECT COUNT(*) AS products_count FROM silver_products").show()
spark.sql("SELECT COUNT(*) AS orders_count FROM silver_orders").show()

# COMMAND ----------

customers = spark.read.table("silver_customers")
orders = spark.read.table("silver_orders")
products = spark.read.table("silver_products")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     o.order_id,
# MAGIC     o.order_date,
# MAGIC     c.customer_name,
# MAGIC     p.product_name,
# MAGIC     p.category,
# MAGIC     o.quantity,
# MAGIC     p.unit_price,
# MAGIC     ROUND(o.quantity * p.unit_price, 2) AS line_revenue
# MAGIC FROM silver_orders o
# MAGIC JOIN silver_customers c
# MAGIC     ON o.customer_id = c.customer_id
# MAGIC JOIN silver_products p
# MAGIC     ON o.product_id = p.product_id;

# COMMAND ----------

# MAGIC %md
# MAGIC This shows a complete transactional view by combining orders with customers and product details. We also get to see the line venuewhich represents the monetary value of ech order line.This is important because it transforms raw transactional data into useful business-ready revenue metrics used for reporting. 

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     c.customer_name,
# MAGIC     ROUND(SUM(o.quantity * p.unit_price), 2) AS total_revenue
# MAGIC FROM silver_orders o
# MAGIC JOIN silver_customers c
# MAGIC     ON o.customer_id = c.customer_id
# MAGIC JOIN silver_products p
# MAGIC     ON o.product_id = p.product_id
# MAGIC GROUP BY c.customer_name
# MAGIC ORDER BY total_revenue DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC This query shows aggregates of each customer's spending, which helps us in identifyig high-value customers. This is an important calculation as it is useful for customer segmentation, loyalty programs and targeted marketing efforts toward the most profitable customers.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     p.category,
# MAGIC     ROUND(SUM(o.quantity * p.unit_price), 2) AS total_revenue
# MAGIC FROM silver_orders o
# MAGIC JOIN silver_products p
# MAGIC     ON o.product_id = p.product_id
# MAGIC GROUP BY p.category
# MAGIC ORDER BY total_revenue DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC Product categories that generate the most revenue are shown with this query, these are the top performers of the business. This calculation is very important for inventory planning, category perfomance tracking and stratigic business decisions around product focus.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC     c.customer_name,
# MAGIC     ROUND(SUM(o.quantity * p.unit_price), 2) AS total_spend
# MAGIC FROM silver_orders o
# MAGIC JOIN silver_customers c
# MAGIC     ON o.customer_id = c.customer_id
# MAGIC JOIN silver_products p
# MAGIC     ON o.product_id = p.product_id
# MAGIC GROUP BY c.customer_name
# MAGIC ORDER BY total_spend DESC
# MAGIC LIMIT 5;

# COMMAND ----------

# MAGIC %md
# MAGIC This query identifiesthe top 5 customers by total amount spent. These customers are important for retention strategies, VIP programs, and revenue stability analysis.