# Databricks notebook source
# MAGIC %md
# MAGIC # Data Cleaning

# COMMAND ----------

# MAGIC %md
# MAGIC ### Caricamento file

# COMMAND ----------

from pyspark.sql.functions import *
import pandas as pd

# percorso del dataset
FILE_PATH = "/Volumes/workspace/sales_project_schema/sales_project_volume/dataset_sales_operations_DIRTY.xlsx"

# caricamento delle tabelle
dates = pd.read_excel(FILE_PATH, sheet_name="dim_date")
customers = pd.read_excel(FILE_PATH, sheet_name="dim_customer")
products = pd.read_excel(FILE_PATH, sheet_name="dim_product")
orders = pd.read_excel(FILE_PATH, sheet_name="fact_orders")
order_lines = pd.read_excel(FILE_PATH, sheet_name="fact_order_lines")
returns = pd.read_excel(FILE_PATH, sheet_name="fact_returns")

# trasformo in DataFrame spark
df_dates = spark.createDataFrame(dates)
df_customers = spark.createDataFrame(customers)
df_products = spark.createDataFrame(products)
df_orders = spark.createDataFrame(orders)
df_order_lines = spark.createDataFrame(order_lines)
df_returns = spark.createDataFrame(returns)

# definisco le copie dei DataFrame
df_dates_bronze = df_dates
df_customers_bronze = df_customers
df_products_bronze = df_products
df_orders_bronze = df_orders
df_order_lines_bronze = df_order_lines
df_returns_bronze = df_returns

print("File caricati con successo!")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Visualizzazione struttura delle tabelle

# COMMAND ----------


# TABELLA DATES
print("Tabella Dates")
print(f"-- Numero di righe: {df_dates_bronze.count()} \n-- Numero di colonne: {len(df_dates_bronze.columns)}")
null_counts = df_dates_bronze.select([
    sum(when(col(c).isNull(), 1).otherwise(0)).alias(c)
    for c in df_dates_bronze.columns
]).collect()[0].asDict()

for colonna, nulli in null_counts.items():
    print(f"{colonna}: {nulli}")

# TABELLA CUSTOMERS
print("------------------------------------------")
print("Tabella Customers")
print(f"-- Numero di righe: {df_customers_bronze.count()} \n-- Numero di colonne: {len(df_customers_bronze.columns)}")
null_counts = df_customers_bronze.select([
    sum(when(col(c).isNull(), 1).otherwise(0)).alias(c)
    for c in df_customers_bronze.columns
]).collect()[0].asDict()

for colonna, nulli in null_counts.items():
    print(f"{colonna}: {nulli}")

# TABELLA PRODUCTS
print("------------------------------------------")
print("Tabella Products")
print(f"-- Numero di righe: {df_products_bronze.count()} \n-- Numero di colonne: {len(df_products_bronze.columns)}")
null_counts = df_products_bronze.select([
    sum(when(col(c).isNull(), 1).otherwise(0)).alias(c)
    for c in df_products_bronze.columns
]).collect()[0].asDict()

for colonna, nulli in null_counts.items():
    print(f"{colonna}: {nulli}")

# TABELLA ORDERS
print("------------------------------------------")
print("Tabella Orders")
print(f"-- Numero di righe: {df_orders_bronze.count()} \n-- Numero di colonne: {len(df_orders_bronze.columns)}")
null_counts = df_orders_bronze.select([
    sum(when(col(c).isNull(), 1).otherwise(0)).alias(c)
    for c in df_orders_bronze.columns
]).collect()[0].asDict()

for colonna, nulli in null_counts.items():
    print(f"{colonna}: {nulli}")

# TABELLA ORDER LINES
print("------------------------------------------")
print("Tabella Order Lines")
print(f"-- Numero di righe: {df_order_lines_bronze.count()} \n-- Numero di colonne: {len(df_order_lines_bronze.columns)}")
null_counts = df_order_lines_bronze.select([
    sum(when(col(c).isNull(), 1).otherwise(0)).alias(c)
    for c in df_order_lines_bronze.columns
]).collect()[0].asDict()

for colonna, nulli in null_counts.items():
    print(f"{colonna}: {nulli}")

# TABELLA RETURNS
print("------------------------------------------")
print("Tabella Returns")
print(f"-- Numero di righe: {df_returns_bronze.count()} \n-- Numero di colonne: {len(df_returns_bronze.columns)}")
null_counts = df_returns_bronze.select([
    sum(when(col(c).isNull(), 1).otherwise(0)).alias(c)
    for c in df_returns_bronze.columns
]).collect()[0].asDict()

for colonna, nulli in null_counts.items():
    print(f"{colonna}: {nulli}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Analisi esplorativa
# MAGIC
# MAGIC In questo primo blocco ho analizzato la struttura delle tabelle, andando a trovare il numero di righe e colonne, il tipo del dato ed il numero di valori nulli per ogni colonna.
# MAGIC
# MAGIC `Tabella Dates`
# MAGIC
# MAGIC La tabella è composta da `731 record` e `14 colonne`.
# MAGIC Non sono presenti valori nulli.
# MAGIC
# MAGIC `Tabella Customers`
# MAGIC
# MAGIC La tabella è composta da `203 record` e `13 colonne`.
# MAGIC Le seguenti colonne hanno valori valori nulli:
# MAGIC - **settore ->** `5` valori nulli
# MAGIC - **canale_acquisizione ->** `10` valori nulli
# MAGIC - **rating_credito ->** `7` valori nulli
# MAGIC
# MAGIC `Tabella Products`
# MAGIC
# MAGIC La tabella è composta da `27 record` e `10 colonne`.
# MAGIC Non sono presenti valori nulli.
# MAGIC
# MAGIC `Tabella Orders`
# MAGIC
# MAGIC La tabella è composta da `1428 record` e `12 colonne`.
# MAGIC Le seguenti colonne hanno valori valori nulli:
# MAGIC - **canale_vendita ->** `17` valori nulli
# MAGIC - **metodo_pagamento ->** `10` valori nulli
# MAGIC - **giorni_consegna ->** `170` valori nulli
# MAGIC
# MAGIC `Tabella Orders_lines`
# MAGIC
# MAGIC La tabella è composta da `2923 record` e `10 colonne`.
# MAGIC Le seguenti colonne hanno valori valori nulli:
# MAGIC - **margine_euro ->** `15` valori nulli
# MAGIC
# MAGIC `Tabella Returns`
# MAGIC
# MAGIC La tabella è composta da `76 record` e `9 colonne`.
# MAGIC Le seguenti colonne hanno valori valori nulli:
# MAGIC - **importo_rimborsato ->** `4` valori nulli
# MAGIC
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pulizia Tabella `Customers`

# COMMAND ----------

print(f"Numero di righe prima della pulizia: {df_customers_bronze.count()}")

# Eliminazione dei valori nulli
# sostituisco il valore null con Sconosciuto
df_customers_silver = df_customers_bronze.fillna("Sconosciuto", subset=["settore", "canale_acquisizione", "rating_credito"])

# Sistemazione incosistenza valori nella colonna "regione"
# mappa regioni
mappa_regioni = {
  "NORD OVEST": "Nord Ovest", "nord ovest": "Nord Ovest" , "N. Ovest" : "Nord Ovest",
  "nord est": "Nord Est", "NORD EST": "Nord Est", "NordEst": "Nord Est", "Nord-Est": "Nord Est",
  "Centro Italia" : "Centro", "centro": "Centro", "CENTRO" : "Centro",
  "sud": "Sud", "SUD": "Sud", "Sud Italia": "Sud",
  "ISOLE": "Isole", "isole": "Isole"
}

condition = col("regione")
# # Sostituisce il valore errato con il corrispondente nella mappa
for wrong_value, correct_value in mappa_regioni.items():
  condition = when(col("regione") == wrong_value, correct_value).otherwise(condition)

df_customers_silver = df_customers_silver.withColumn("regione", condition)

# Gestione email
# rimuove gli spazi
# rimuove il doppio punto
# aggiunge @ dove manca
df_customers_silver = df_customers_silver \
  .withColumn("email_contatto", regexp_replace(col("email_contatto"), " ", "")) \
  .withColumn("email_contatto", regexp_replace(col("email_contatto"), r'\.{2,}', '.')) \
  .withColumn("email_contatto", \
    when(~col("email_contatto").contains("@"), 
      regexp_replace(col("email_contatto"), r'(acquisti)(.*)', "$1@$2")
    ).otherwise(col("email_contatto"))
  )

# Rimozione duplicati
df_customers_silver = df_customers_silver.drop_duplicates(subset=["partita_iva"])

print(f"Numero di righe dopo la pulizia: {df_customers_silver.count()}")

display(df_customers_silver)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pulizia Tabella `Products`

# COMMAND ----------

print(f"Record prima della pulizia: {df_products_bronze.count()}")

# Sostituzione valore anomalo product_id = P011, nella colonna margine_lordo_pct
# Sostituzione valore negativo in prezzo_listino_2023 con product_id = P025
# Imposto come valore di fallback nella colonna prezzo_listino_2024 con product_id = P012 uguale a quello del prezzo_listino_2023
df_products_silver = df_products_bronze \
    .withColumn("margine_lordo_pct",
        when(col("product_id") == "P011", 0.43
        ).otherwise(col("margine_lordo_pct"))) \
    .withColumn("prezzo_listino_2023", 
        when(col("product_id") == "P025", 189
        ).otherwise(col("prezzo_listino_2023"))) \
    .withColumn("prezzo_listino_2024", 
        when(col("product_id") == "P012", 599
        ).otherwise(col("prezzo_listino_2024")))

print(f"Record dop la pulizia: {df_products_silver.count()}")

display(df_products_silver)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pulizia Tabella `Orders`

# COMMAND ----------

print(f"Ordini prima della pulizia: {df_orders_bronze.count()}")

# Eliminazione dei valori nulli
df_orders_silver = df_orders_bronze.fillna("Sconosciuto", subset=["canale_vendita", "metodo_pagamento"])

# Il valore null nell colonna giorni_consegna è stato sostituito con il valore della mediana per i valori di quella colonna
median_days = df_orders_silver.approxQuantile("giorni_consegna", [0.5], 0.0)[0]

df_orders_silver = df_orders_silver \
  .withColumn("giorni_consegna",
    when(col("giorni_consegna").isNull(), median_days
    ).otherwise(col("giorni_consegna")))
  
# verifico che la data_ordine sia in formato datetime
df_orders_silver = df_orders_silver.withColumn("data_ordine", to_date((col("data_ordine"))))

# mappa con order_id e data corretta
correzioni = {
  "ORD00049": "2023-01-27",
  "ORD00326": "2023-06-12",
  "ORD00337": "2023-06-18",
  "ORD00440": "2023-09-05",
  "ORD00719": "2024-01-02"
}

condition = col("data_ordine")

# applico la correzione
for order_id, data_corretta in correzioni.items():
  condition = when(
    col("order_id") == order_id, to_date(lit(data_corretta), "yyyy-MM-dd")
    ).otherwise(condition)

df_orders_silver = df_orders_silver.withColumn("data_ordine", condition)

# Verifico i customer_id che non sono presenti nella tabella df_customers_bronze
valid_customers = df_customers_silver.select("customer_id").distinct()
id_customers_orders = df_orders_silver.select("customer_id").distinct()

id_inconsistenti = id_customers_orders.subtract(valid_customers)

print(f"customer_id orfani: {id_inconsistenti.count()}")
display(id_inconsistenti)

# elimino gli ordini con customer_id inconsistenti
df_orders_silver = df_orders_silver.join(valid_customers, on="customer_id", how="inner")

# elimino id duplicati
df_orders_silver = df_orders_silver.drop_duplicates(["order_id"])

# Verifica incoerenza importo_totale vs netto + iva
df_orders_silver = df_orders_silver \
  .withColumn("totale_atteso", round(col("importo_netto") + col("iva"), 2)) \
  .withColumn("totale_incoerente", col("importo_totale") != col("totale_atteso"))

# sostituzione con il valore corretto
df_orders_silver = df_orders_silver \
  .withColumn("importo_totale", col("totale_atteso"))

# elimino le colonne di appoggio create per effettuare la correzione
df_orders_silver = df_orders_silver.drop("totale_atteso", "totale_incoerente")

print(f"Ordini dopo la pulizia: {df_orders_silver.count()}")

display(df_orders_silver)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pulizia Tabella `Orders_lines`

# COMMAND ----------

print(f"Numero di record prima della puliza: {df_order_lines_bronze.count()}")

# Eliminazione dei valori nulli
# Il valore null nell colonna margine_euro è stato sostituito con il valore della mediana per i valori di quella conna
median_margin = df_order_lines_bronze.approxQuantile("margine_euro", [0.5], 0.0)[0]
df_order_lines_silver = df_order_lines_bronze.fillna({"margine_euro": median_margin})

# gestione valori negativi nella colonna quantita
df_order_lines_silver = df_order_lines_silver \
  .withColumn("quantita", abs(col("quantita")))

# gestione della quantità = 0
# ID: OL0049001, OL0105501, OL0115201, OL0122702
mappa_quantita = {
    "OL0049001": 1,
    "OL0105501": 1,
    "OL0115201": 2,
    "OL0122702": 1
}

condition = col("quantita")

# applico la correzione
for order_line_id, quantita_corretta in mappa_quantita.items():
  condition = when(col("order_line_id") == order_line_id, quantita_corretta).otherwise(condition)

df_order_lines_silver = df_order_lines_silver.withColumn("quantita", condition)

# verifico i product_id inconsistenti
valid_product_id = df_products_silver.select("product_id").distinct()
id_products_orders_lines = df_order_lines_silver.select("product_id").distinct()
id_inconsistenti_ol = id_products_orders_lines.subtract(valid_product_id)

print(f"Id inconsistenti: {id_inconsistenti_ol.count()}")
display(id_inconsistenti_ol)

# elimino i record con il product_id inconsistente
df_order_lines_silver = df_order_lines_silver.join(valid_product_id, on="product_id", how="inner")

# verifico gli importi netti superiori agli importi lordi
invalid_imports = df_order_lines_silver.filter(col("importo_netto") > col("importo_lordo"))
print(f"Record con importo_netto > importo_lordo: {invalid_imports.count()}")

# elimino i record con importi netti superiori agli importi lordi
df_order_lines_silver = df_order_lines_silver.filter(col("importo_netto") <= col("importo_lordo"))

# Gestione outlier del prezzo unitario: colonna prezzo_unitario
min_price = df_order_lines_silver.agg(min(col("prezzo_unitario"))).collect()[0][0]
max_price = df_order_lines_silver.agg(max(col("prezzo_unitario"))).collect()[0][0]

print(f"Prezzo unitario minimo: {min_price}")
print(f"Prezzo unitario massimo: {max_price}")

prezzi_oltre_2000 = df_order_lines_silver.filter(col("prezzo_unitario") > 1000)
print(f"Prezzi unitari > 1000: {prezzi_oltre_2000.count()}")

# elimino i record con prezzo_unitario maggiore di 1000
df_order_lines_silver = df_order_lines_silver.filter(col("prezzo_unitario") < 1000)

print(f"Numero di record dopo la puliza: {df_order_lines_silver.count()}")

display(df_order_lines_silver)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Pulizia Tabella `Returns`

# COMMAND ----------

print(f"Numero di record prima della pulizia: {df_returns_bronze.count()}")

# Eliminazione dei valori nulli
# sostituisco il valore null con 0
df_returns_silver = df_returns_bronze.fillna({"importo_rimborsato": 0})

# Sistemazione incosistenza valori nella colonna "motivo_reso"
# mappa motivi
mappa_motivi = {
  "trasporto": "Danno trasporto", "PRODOTTO DIFETTOSO": "Prodotto difettoso",
  "CAMBIO IDEA": "Cambio idea", "Sbagliato": "Ordine errato", "ho cambiato idea": "Cambio idea",
  "non funziona": "Prodotto difettoso", "non conforme alle aspettative": "Non conforme",
  "Danno nel traporto": "trasporto"
}

condition = col("motivo_reso")

# sotituisco i valori inconsistenti
for valore_errato, valore_corretto in mappa_motivi.items():
    condition = when(
        col("motivo_reso") == valore_errato, valore_corretto
    ).otherwise(condition)

df_returns_silver = df_returns_silver.withColumn("motivo_reso", condition)

# Gestione data_reso precedente alla data_ordine
# effettuto il join tra la tabella Returns e Orders
df_join = df_returns_silver.join(
  df_orders_silver.select("order_id", "data_ordine"),
  on="order_id",
  how="left"
)

invalid_dates = df_join.filter(col("data_reso") < col("data_ordine"))
print(f"Resi con data precedente alla data dell'ordine: {invalid_dates.count()}")

# estraggo gli id dei resi con data errata
invalid_return_ids = invalid_dates.select("return_id")

# elimino i record
df_returns_silver = df_returns_silver.join(
  invalid_return_ids,
  on="return_id",
  how="left_anti"
)

df_returns_silver = df_returns_silver.withColumn("data_reso", to_date(col("data_reso")))

print(f"Numero di record dopo la pulizia: {df_returns_silver.count()}")

display(df_returns_silver)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Verifica dei dati

# COMMAND ----------

def assert_check(cond, msg):
    status = "✓" if cond else "✗ FAIL"
    print(f"  {status}  {msg}")

# ── dim_customer ──
print("── dim_customer ──")

# customer_id univoci
total = df_customers_silver.count()
distinct = df_customers_silver.select("customer_id").distinct().count()
assert_check(total == distinct, "customer_id univoci")

# Regioni nel set canonico
regioni_valide = ["Nord Ovest", "Nord Est", "Centro", "Sud", "Isole"]
regioni_invalide = df_customers_silver.filter(~col("regione").isin(regioni_valide)).count()
assert_check(regioni_invalide == 0, "Regioni nel set canonico")

# Nessun settore nullo
settori_nulli = df_customers_silver.filter(col("settore").isNull()).count()
assert_check(settori_nulli == 0, "Nessun settore nullo")

# ── fact_orders ──
print("── fact_orders ──")

# order_id univoci
total = df_orders_silver.count()
distinct = df_orders_silver.select("order_id").distinct().count()
assert_check(total == distinct, "order_id univoci")

# Importi >= 0
importi_negativi = df_orders_silver.filter(col("importo_netto") < 0).count()
assert_check(importi_negativi == 0, "Importi >= 0")

# ── fact_order_lines ──
print("── fact_order_lines ──")

# Quantità > 0
quantita_invalide = df_order_lines_silver.filter(
    col("quantita").isNotNull() & (col("quantita") <= 0)
).count()
assert_check(quantita_invalide == 0, "Quantità > 0")

# Netto <= Lordo
incoerenti = df_order_lines_silver.filter(
    col("importo_netto") > col("importo_lordo")
).count()
assert_check(incoerenti == 0, "Netto <= Lordo")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Panoramica pulizia
# MAGIC
# MAGIC `Tabella Customers`
# MAGIC
# MAGIC In questa tabella si sono riscontrati diversi valori anomali e si è proceduto nel seguente modo:
# MAGIC
# MAGIC - Nelle colonne `settore`, `canale_acquisizione` e `rating_credito` erano presenti diversi record con valore `null`. Ho sostituito il null con `Sconosciuto`.
# MAGIC - Inoltre nella colonna `regione` era presenti valori incosistenti (case, trattini o abbreviazioni). Essi sono stati mappati e sostiuiti con i valori corretti.
# MAGIC - Presenza nella colonna `email_contatto `di email malformate (spazi, doppi punti e @ mancante). Ho proceduto con la correzioni di tali valori.
# MAGIC - Infine erano presenti delle righe duplicate. ID del cliente diverso con la stessa Partita IVA. Sono stati rimossi i valori duplicati e tenuti sono quelli originali.
# MAGIC
# MAGIC `Tabella Products`
# MAGIC
# MAGIC - Nella tabella in questione abbiamo un campo con un `margine_lordo_pct` superiore al **100%** (`P011`). Quetso potrebbe significare la presenza di qualche outlier o molto probabilmente un errore di battiture. Dopo aver analizzato i dati ed effettuato delle verifiche si è giunti alla conclusione che si tratti di un errore di battitura.
# MAGIC Il calcolo della percentuale viene fatto tra il prezzo di listino del 2023 e il costo di acquisto del prodotto. Di conseguenza si è deciso di sostituire il valore in con il calcolo corretto.
# MAGIC - Un ulteriore errore di battitura lo abbiamo sulla colonna `prezzo_listino_2023` con `product_id` `P025`. E' stato inserito un valore negativo (impossibile), di conseguenza ho sostituito con il valore errato con il quello corretto.
# MAGIC - Un valore anomalo è stato riscontrato nella colonna `prezzo_listino_2024` con `product_id` `P012`. Nel **2023** il prezzo era di **599**, invece del **2024** è **0**. Si è deciso di impostare come valore di fallback lo stesso che era presente nel 2023.
# MAGIC
# MAGIC `Tabella Orders`
# MAGIC
# MAGIC Le operazioni di pulizia nella tabella **Orders** sono stati i seguenti:
# MAGIC - I valori `null` nelle colonne `canale_vendita` e `metodo_pagamento` sono stati sostituiti con `Sconosciuto` e nella colonna `giorni_consegna` con il valore della `mediana` per quella colonna.
# MAGIC - I campi della colonna `data_ordine` sono stati convertiti in formato `datetime`. Inoltre sono state trovate delle date inconsistenti con valori 2033 e 2034 nelle date. Trattandosi di errori di battitura essi sono stati sostituiti con i valori corretti.
# MAGIC - I record con `customer_id` presenti nella tabella Orders ma **non** in quella Customers sono stati `eliminati`.
# MAGIC - Eliminazione di `order_id` duplicati.
# MAGIC - Correzione dei valori incoerenti nel calcolo dell'`importo_totale`
# MAGIC
# MAGIC `Tabella Orders_lines`
# MAGIC
# MAGIC Nella seguente tabella sono state effettuale le seguenti correzioni:
# MAGIC - Nella colonna `margine_euro` erano presenti alcuni valori `null`, essi sono stati sostituiti con il valore della `mediana` per i valori di quella colonna.
# MAGIC - Nella colonna quantita erano presenti `5 record` con quantità **negativa** e `4` con **quantità = 0**. Le quantità negative sono state corrette utilizzano il **valore assoluto** così da eliminare il segno - davanti, mentre le quantità 0, sono stati inseriti i valori corretti controllando che il prezzo unitario e l'importo lordo corrispondesse.
# MAGIC - Sono stati trovati **valori negativi o uaguali a 0** nella colonna `quantita`. I valori negativi sono stati trasformati utilizzando il `valore assoluto` per quel valore, la quantità = 0 è stata sostituita con il valore corretto analizzando il prezzo per ogni singola unità e l'importo lordo.
# MAGIC - Sono stati trovati campi nella colonna product_id che non sono presenti nella tabella Products (`P991`, `P976`, `P999`, `P916`, `P994`). I record con questi valori sono stati successivamente rimossi poichè sono dati inconsistenti e inutilizzabili.
# MAGIC - Nella tabella colonna `prezzo_uniario` sono stati trovati `3` valori **superiori** a `1000`. Essi sono considerati degli **outlier**, pertanto i record con tali valori sono stati **eliminati**.
# MAGIC
# MAGIC `Tabella Returns`
# MAGIC
# MAGIC Nella seguente tabella è state effettuate le seguenti operazioni di pulizia:
# MAGIC - Nella colonna `importo_rimborsato` erano presenti alcuni valori `null`, sostituiti successivamente con il valore default `0`.
# MAGIC - Erano presenti motivi di reso insconsistenti nella colonna `motivo_reso`. Case e sinonimi diversi, sono stati sostituiti con delle stringhe standardardizzate.
# MAGIC - Nella colonna `data_reso` erano presenti delle date **precedenti** alla data in cui era stato effettuato il reso (impossibile). I record con queste date sono stati **eliminati**.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scrittura in formato `parquet`

# COMMAND ----------

SILVER_PATH = "/Volumes/workspace/sales_project_schema/sales_project_volume/silver/"

df_customers_silver.write.format("parquet").save(f"{SILVER_PATH}/customers_silver")
print("Creazione del file customers_silver.parquet avvenuto con successo")

df_dates_bronze.write.format("parquet").save(f"{SILVER_PATH}/dates_silver")
print("Creazione del file dates_silver.parquet avvenuto con successo")

df_products_silver.write.format("parquet").save(f"{SILVER_PATH}/products_silver")
print("Creazione del file products.parquet avvenuto con successo")

df_orders_silver.write.format("parquet").save(f"{SILVER_PATH}/orders_silver")
print("Creazione del file orders_silver.parquet avvenuto con successo")

df_order_lines_silver.write.format("parquet").save(f"{SILVER_PATH}/order_lines_silver")
print("Creazione del file order_lines_silver.parquet avvenuto con successo")

df_returns_silver.write.format("parquet").save(f"{SILVER_PATH}/returns_silver")
print("Creazione del file returns.parquet avvenuto con successo")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Scrittura in formato Delta

# COMMAND ----------

SILVER_DELTA_PATH = "/Volumes/workspace/sales_project_schema/sales_project_volume/delta"

# df_customers_silver.write.saveAsTable("workspace.sales_project_schema.customers_silver")
# df_dates_bronze.write.saveAsTable("workspace.sales_project_schema.dates_silver")
# df_products_silver.write.saveAsTable("workspace.sales_project_schema.products_silver")
# df_orders_silver.write.saveAsTable("workspace.sales_project_schema.orders_silver")
# df_order_lines_silver.write.saveAsTable("workspace.sales_project_schema.order_lines_silver")
# df_returns_silver.write.saveAsTable("workspace.sales_project_schema.returns_silver")

df_dates_bronze.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(f"{SILVER_DELTA_PATH}/dates")

# scrittura tabella Customers
df_customers_silver.write.format("delta").mode("overwrite").option("overwriteSchema", "true").partitionBy("regione").save(f"{SILVER_DELTA_PATH}/customers")

# scrittura tabella Products
df_products_silver.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(f"{SILVER_DELTA_PATH}/products")

# scrittura tabella Orders
df_orders_silver = df_orders_silver \
    .withColumn("anno", year("data_ordine")) \
    .withColumn("mese_num", month("data_ordine"))

df_orders_silver.write.format("delta").mode("overwrite").option("overwriteSchema", "true").partitionBy("anno", "mese_num").save(f"{SILVER_DELTA_PATH}/orders")

# scrittura tabella Order Lines
df_order_lines_silver.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(f"{SILVER_DELTA_PATH}/order_lines")

# scrittura tabella Returns
df_returns_silver.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(f"{SILVER_DELTA_PATH}/returns")

print("Tutte le tabelle Delta scritte correttamente.")

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE workspace.sales_project_schema.dim_customers_silver;
# MAGIC DROP TABLE workspace.sales_project_schema.dim_dates_silver;
# MAGIC DROP TABLE workspace.sales_project_schema.dim_order_lines_silver;
# MAGIC DROP TABLE workspace.sales_project_schema.dim_orders_silver;
# MAGIC DROP TABLE workspace.sales_project_schema.dim_products_silver;
# MAGIC DROP TABLE workspace.sales_project_schema.dim_returns_silver;
# MAGIC

# COMMAND ----------

orders = spark.table("workspace.sales_dw.orders_silver")
order_lines = spark.table("workspace.sales_dw.order_lines_silver")
returns = spark.table("workspace.sales_dw.returns_silver")

display(orders)
display(order_lines)
display(returns)
