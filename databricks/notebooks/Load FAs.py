# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC ## Cargar Datos de Agentes Libres
# MAGIC
# MAGIC Este notebook se encarga actualizar la tabla con los datos de los Agentes Libres extraidos de Overthecap. Tan sólo se ejecuta cuando quieres refrescar los que han firmado/renovado o si se han borrado las tablas.
# MAGIC
# MAGIC ### Pre-procesado manual
# MAGIC Los datos se extraen manualmente de la web siguiendo este proceso:
# MAGIC
# MAGIC 1. Ve a la web de [Overthecap](https://overthecap.com/free-agency), selecciona toda la tabla y copia.
# MAGIC 2. Pega el contenido en Excel y guárdalo como CSV
# MAGIC 3. Desde Catalog, sube el fichero a `/FileStore/tables/nfl/`

# COMMAND ----------

# File location and type
file_location = "/FileStore/tables/nfl/overthecap_freeagents_20250213.csv"
file_type = "csv"

# The applied options are for CSV files. For other file types, these will be ignored.
df = spark.read.format(file_type) \
  .option("inferSchema", "true") \
  .option("header", "true") \
  .option("mergeSchema", "true") \
  .option("sep", ";") \
  .load(file_location)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conversiones
# MAGIC
# MAGIC En esta seccion se implementan las distintas conversiones de los datos.

# COMMAND ----------

from pyspark.sql.functions import translate, col
from pyspark.sql.types import IntegerType

# Rename columns with unsupported names by Delta
df = df.withColumnRenamed("Pos.", "Pos") \
       .withColumnRenamed("2024 Team", "PrevTeam") \
       .withColumnRenamed("2025 Team", "NextTeam") \
       .withColumnRenamed("Snaps", "Snaps%") \
       .withColumnRenamed("Current APY", "SalaryPY")

# Convert string currency to int
df = df.withColumn('Guarantees', translate(col("Guarantees"), "$,", "").cast(IntegerType())) \
       .withColumn('SalaryPY', translate(col("SalaryPY"), "$,", "").cast(IntegerType())) \
       .withColumn('Snaps%', translate(col("Snaps%"), "%", "").cast(IntegerType()))

display(df)
df.printSchema()


# COMMAND ----------

# MAGIC %md
# MAGIC En caso de tener que borrar algun fichero, usa dbutils como en el siguiente bloque:
# MAGIC ```
# MAGIC dbutils.fs.rm("dbfs:/user/hive/warehouse/dir", True)
# MAGIC ```
# MAGIC El segundo parametro es para borrado recursivo.

# COMMAND ----------

# Delete un-used files/folders
#dbutils.fs.rm("dbfs:/FileStore/tables/nfl/pff/defense_summary.csv", True)
dbutils.fs.ls("dbfs:/FileStore/tables/nfl/pff/")


# COMMAND ----------

# MAGIC %md
# MAGIC ## Actualiza Tabla
# MAGIC
# MAGIC Crea la Delta table con todos los datos y ciertas conversiones que me los hacen más manejables.

# COMMAND ----------

permanent_table_name = "freeagents_2025"

df.write.format("delta").saveAsTable(permanent_table_name, mode="overwrite")

#df.write.format("delta").mode("overwrite").save(delta_table_path)
#df.write.format("delta").mode("append").save(delta_table_path)

# COMMAND ----------

# MAGIC %md
# MAGIC A quick test to view the result:

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from freeagents_2025 where Pos == "EDGE";