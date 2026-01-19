# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Cargar Datos de PFF
# MAGIC
# MAGIC Este notebook se encarga actualizar las tablas con los datos de Pro Football Focus. Tan sólo se ejecuta cuando quieres refrescar los datos de una temporada.
# MAGIC
# MAGIC ### Pre-procesado manual
# MAGIC Los datos se extraen manualmente de la web siguiendo este proceso:
# MAGIC
# MAGIC 1. Ve a la web de [PFF](https://premium.pff.com/nfl/positions/2024/REGPO), selecciona una categoría y luego pulsa CSV para descargar un fichero con los datos.
# MAGIC 2. Desde Catalog, sube el fichero a `/FileStore/tables/nfl/pff/`

# COMMAND ----------

# MAGIC %md
# MAGIC ## Defensa

# COMMAND ----------

# File location and type
file_location = "/FileStore/tables/nfl/pff/defense_summary.csv" 
file_type = "csv"

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", "true") \
  .option("header", "true") \
  .option("mergeSchema", "true") \
  .option("sep", ",") \
  .load(file_location)

# Delta Table
permanent_table_name = "defense_summary"
df.write.format("delta").saveAsTable(permanent_table_name, mode="overwrite")  

# COMMAND ----------

# MAGIC %md
# MAGIC ## Ataque - Bloqueos

# COMMAND ----------

# File location and type
file_location = "/FileStore/tables/nfl/pff/offense_blocking.csv" 
file_type = "csv"

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", "true") \
  .option("header", "true") \
  .option("mergeSchema", "true") \
  .option("sep", ",") \
  .load(file_location)

# Delta Table
permanent_table_name = "offense_blocking"
df.write.format("delta").saveAsTable(permanent_table_name, mode="overwrite")  

# COMMAND ----------

# MAGIC %md
# MAGIC ## Retornadores

# COMMAND ----------

# File location and type
file_location = "/FileStore/tables/nfl/pff/return_summary.csv" 
file_type = "csv"

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", "true") \
  .option("header", "true") \
  .option("mergeSchema", "true") \
  .option("sep", ",") \
  .load(file_location)

# Delta Table
permanent_table_name = "return_summary"
df.write.format("delta").saveAsTable(permanent_table_name, mode="overwrite")  

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC ## Receptores

# COMMAND ----------

# File location and type
file_location = "/FileStore/tables/nfl/pff/receiving_summary.csv" 
file_type = "csv"

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", "true") \
  .option("header", "true") \
  .option("mergeSchema", "true") \
  .option("sep", ",") \
  .load(file_location)

# Delta Table
permanent_table_name = "receiving_summary"
df.write.format("delta").saveAsTable(permanent_table_name, mode="overwrite")  

# COMMAND ----------

# MAGIC %md
# MAGIC ## Corredores

# COMMAND ----------

# File location and type
file_location = "/FileStore/tables/nfl/pff/rushing_summary.csv" 
file_type = "csv"

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", "true") \
  .option("header", "true") \
  .option("mergeSchema", "true") \
  .option("sep", ",") \
  .load(file_location)

# Delta Table
permanent_table_name = "rushing_summary"
df.write.format("delta").saveAsTable(permanent_table_name, mode="overwrite")  

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC #QB

# COMMAND ----------

# File location and type
file_location = "/FileStore/tables/nfl/pff/passing_summary.csv" 
file_type = "csv"

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", "true") \
  .option("header", "true") \
  .option("mergeSchema", "true") \
  .option("sep", ",") \
  .load(file_location)

# Delta Table
permanent_table_name = "passing_summary"
df.write.format("delta").saveAsTable(permanent_table_name, mode="overwrite")  