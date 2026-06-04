# Databricks notebook source
# MAGIC %md
# MAGIC ## 1. Setup e caricamento

# COMMAND ----------

from pyspark.sql import *
from pyspark.sql.functions import *

spark = SparkSession.builder.getOrCreate()

# Silver path
DELTA_TABLE_PATH = "workspace.sales_dw"

# Lettura tebelle silver
orders = spark.table(f"{DELTA_TABLE_PATH}.orders_silver")
order_lines = spark.table(f"{DELTA_TABLE_PATH}.order_lines_silver")
returns = spark.table(f"{DELTA_TABLE_PATH}.returns_silver")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Creazione Tabelle Gold

# COMMAND ----------

# Applico un flag per gli ordini che sono stati rimborsati
return_flags = (
    returns
    .filter(col("stato_reso").isin("Approvato", "Rimborsato"))
    .select("order_id", "product_id")
    .distinct()
    .withColumn("is_returned", lit(True))
)

# --------------------------------------------------
# Creazione della Tabella fact_sales_gold
# Effettuo il join tra order_lines e orders
# Escludo gli ordini Annullati (non generano ricavo)
# Aggiungi flag is_returned
fact_sales_gold = order_lines.join(
    orders.select(
        "order_id", 
        "customer_id", 
        "data_id", 
        "data_ordine", 
        "canale_vendita", 
        "stato_ordine", 
        "metodo_pagamento", 
        "giorni_consegna", 
        "anno", 
        "mese_num"
        ),
        on="order_id", how="inner"
    ).filter(col("stato_ordine") != "Annullato").join(return_flags, on=["order_id", "product_id"], how="left")\
        .withColumn("is_returned", coalesce(col("is_returned"), lit(False))) \
        .withColumn("margine_pct", 
                    round(when(col("importo_netto") != 0, col("margine_euro") / col("importo_netto") * 100)
                        .otherwise(None), 2)
        )

# Selezioni le colonne utili per i calcoli kpi
fact_sales_gold = fact_sales_gold.select(
    col("order_line_id"),
    col("order_id"),
    col("product_id"),
    col("customer_id"),
    col("data_id"),
    col("data_ordine"),
    col("anno"),
    col("mese_num"),
    col("canale_vendita"),
    col("stato_ordine"),
    col("metodo_pagamento"),
    col("giorni_consegna"),
    col("quantita"),
    col("prezzo_unitario"),
    col("sconto_pct"),
    col("importo_lordo"),
    col("importo_netto"),
    col("costo_prodotto"),
    col("margine_euro"),
    col("margine_pct"),
    col("is_returned")
)

# --------------------------------------------------
# Creazione della Tabella fact_returns_gold
# Aggiungo la colonna data_id da fact_orders
# Aggiungo anno e mese
# Aggiungo flag is_refunded per resi già rimborsati
orders_for_join = orders.select("order_id", "data_id") \
    .withColumnRenamed("data_id", "data_id_ordine_originale")

fact_returns_gold = returns.join(orders_for_join, on="order_id", how="left") \
    .withColumn("anno_reso", year(col("data_reso").cast("date"))) \
    .withColumn("mese_reso", month(col("data_reso").cast("date"))) \
    .withColumn("is_refunded", col("stato_reso") == "Rimborsato")

fact_returns_gold = fact_returns_gold.select(
    col("return_id"),
    col("order_id"),
    col("product_id"),
    col("customer_id"),
    col("data_id_ordine_originale").alias("data_id"),  # FK → Dim_Dates
    col("data_reso"),
    col("anno_reso"),
    col("mese_reso"),
    col("quantita_resa"),
    col("importo_rimborsato"),
    col("motivo_reso"),
    col("stato_reso"),
    col("is_refunded")
)

display(fact_sales_gold)
display(fact_returns_gold)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Scrittura Tabelle Gold

# COMMAND ----------

# Creazione Tabelle in Formato Delta
fact_sales_gold.write.saveAsTable(f"{DELTA_TABLE_PATH}.fact_sales_gold")
fact_returns_gold.write.saveAsTable(f"{DELTA_TABLE_PATH}.fact_returns_gold")