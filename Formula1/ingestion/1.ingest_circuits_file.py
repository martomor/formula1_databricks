# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Ingest circuits.csv file
# MAGIC #### Step 1 - Read the CSV file using the spark dataframe reader

# COMMAND ----------

dbutils.widgets.help()

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

dbutils.widgets.text("p_file_date", "2021-03-21")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

from pyspark.sql.types import StructType, StringType, IntegerType, DoubleType, StructField

# COMMAND ----------

circuits_schema = StructType(fields=[StructField("circuitId", IntegerType(), False),
                                     StructField("circuitRef", StringType(), True),
                                     StructField("name", StringType(), True),
                                     StructField("location", StringType(), True),
                                     StructField("country", StringType(), True),
                                     StructField("lat", DoubleType(), True),
                                     StructField("lng", DoubleType(), True),
                                     StructField("alt", IntegerType(), True),
                                     StructField("url", StringType(), True)])

# COMMAND ----------

circuits_df = (
    spark.read.option("header", True)
    .schema(circuits_schema)
    .csv(f"{raw_folder_path}/{v_file_date}/circuits.csv")
)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step2 - Select only the required columns

# COMMAND ----------

#circuits_selected_df = circuits_df.select("circuitId","circuitRef","name","location","country","lat","lng","alt")

# COMMAND ----------

#circuits_selected_df = circuits_df.select(circuits_df["circuitId"], circuits_df["circuitRef"], circuits_df["name"], circuits_df["location"], circuits_df["country"], circuits_df["lat"], circuits_df["lng"], circuits_df["alt"])

# COMMAND ----------

#circuits_selected_df = circuits_df.select(circuits_df.circuitId,circuits_df.circuitRef,circuits_df.name,circuits_df.location,circuits_df.country,circuits_df.lat,circuits_df.lng,circuits_df.alt)

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

circuits_selected_df = circuits_df.select(col("circuitId"),col("circuitRef"),col("name"),col("location"),col("country"),col("lat"),col("lng"),col("alt"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step3 - Rename Columns as required

# COMMAND ----------

from pyspark.sql.functions import lit

# COMMAND ----------

circuits_renamed_df = circuits_selected_df.withColumnRenamed("circuitID", "circuit_id") \
                                          .withColumnRenamed("circuitRef", "circuit_ref") \
                                          .withColumnRenamed("lat", "latitude") \
                                          .withColumnRenamed("lng", "longitude")\
                                          .withColumnRenamed("alt", "altitude") \
                                          .withColumn("data_source", lit(v_data_source))\
                                          .withColumn("file_date", lit(v_file_date))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4 - Add Ingestion Date

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

# COMMAND ----------

circuits_final_df = add_ingestion_date(circuits_renamed_df)
#circuits_final_df = circuits_renamed_df.withColumn("ingestion_date", current_timestamp())

#How to hardcode values in columns
#circuits_final_df = circuits_renamed_df.withColumn("ingestion_date", current_timestamp()).withColumn("env", lit("Production"))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 5 - Write data to datalake as parquet

# COMMAND ----------

#circuits_final_df.write.mode("overwrite").parquet(f"{processed_folder_path}/circuits")

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG hive_metastore;
# MAGIC

# COMMAND ----------


circuits_final_df.write.mode("overwrite").format("delta").saveAsTable("f1_processed.circuits")

# COMMAND ----------

dbutils.notebook.exit("Success")
