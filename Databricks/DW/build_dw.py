# Databricks notebook source
# MAGIC %md
# MAGIC ### Crea il database (schema)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE DATABASE IF NOT EXISTS sales_dw;
# MAGIC
# MAGIC USE sales_dw

# COMMAND ----------

# MAGIC %md
# MAGIC ### Legge e carica i volumi per ogni tabella

# COMMAND ----------

DELTA_PATH = "/Volumes/workspace/sales_project_schema/sales_project_volume/delta/"

df_customers = spark.read.format("delta").load(f"{DELTA_PATH}customers/")
df_dates = spark.read.format("delta").load(f"{DELTA_PATH}dates/")
df_products = spark.read.format("delta").load(f"{DELTA_PATH}products/")
df_orders = spark.read.format("delta").load(f"{DELTA_PATH}orders/")
df_order_lines = spark.read.format("delta").load(f"{DELTA_PATH}order_lines/")
df_returns = spark.read.format("delta").load(f"{DELTA_PATH}returns/")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scrittura tabelle in formato delta

# COMMAND ----------

df_customers.write.saveAsTable("workspace.sales_dw.customers_silver")
df_dates.write.saveAsTable("workspace.sales_dw.dates_silver")
df_products.write.saveAsTable("workspace.sales_dw.products_silver")
df_orders.write.saveAsTable("workspace.sales_dw.orders_silver")
df_order_lines.write.saveAsTable("workspace.sales_dw.order_lines_silver")
df_returns.write.saveAsTable("workspace.sales_dw.returns_silver")