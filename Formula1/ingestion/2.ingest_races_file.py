# Databricks notebook source
# MAGIC %md
# MAGIC ### Ingest races.csv file
# MAGIC
# MAGIC

# COMMAND ----------

dbutils.widgets.text("p_data_source", "")
v_data_source = dbutils.widgets.get("p_data_source")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1 - Read the CSV file using the spark dataframe reader API

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

# COMMAND ----------

races_schema = StructType(fields=[StructField("raceId", IntegerType(), False),
                                     StructField("year", IntegerType(), True),
                                     StructField("round", IntegerType(), True),
                                     StructField("circuitId", IntegerType(), True),
                                     StructField("name", StringType(), True),
                                     StructField("date", DateType(), True),
                                     StructField("time", StringType(), True),
                                     StructField("url", StringType(), True)
])

# COMMAND ----------

races_df = spark.read \
    .option("header", True) \
    .schema(races_schema) \
    .csv(f"{raw_folder_path}/races.csv")


# COMMAND ----------

display(races_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2 - Add Ingestion date and race_timestamp to the dataframe

# COMMAND ----------

from pyspark.sql.functions import current_timestamp , to_timestamp, concat, lit, col

# COMMAND ----------

races_with_timestamp_df = races_df.withColumn("ingestion_date", current_timestamp()) \
                                .withColumn("race_timestamp", to_timestamp(concat(col("date"), lit(' '),col('time')), 'yyyy-MM-dd HH:mm:ss'))\
                                .withColumn("data_source", lit(v_data_source))
                                

# COMMAND ----------

display(races_with_timestamp_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3 - Select only the columns required and rename as required

# COMMAND ----------

races_selected_df = races_with_timestamp_df.select(
    col('raceId').alias('race_id'),
    col('year').alias('race_year'),
    col('round'),
    col('circuitId').alias('circuit_id'),
    col('name'),
    col('ingestion_date'),
    col('race_timestamp'),
    col('data_source')
)


# COMMAND ----------

display(races_selected_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Write the output to processed container in parquet format
# MAGIC

# COMMAND ----------

#races_selected_df.write.mode('overwrite').partitionBy('race_year').parquet(f"{processed_folder_path}/races")

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG hive_metastore;

# COMMAND ----------

races_selected_df.write.mode("overwrite").format("parquet").saveAsTable("f1_processed.races")

# COMMAND ----------

#display(spark.read.parquet("/mnt/formula1dlmm24/processed/races"))

# COMMAND ----------

dbutils.notebook.exit("Success")
