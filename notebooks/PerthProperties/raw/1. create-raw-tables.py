# Databricks notebook source
dbutils.widgets.text("p_file_date", "2021-08-01")
v_file_date = dbutils.widgets.get("p_file_date")

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1. Read the csv file using spark dataframe reader

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

property_schema = StructType(fields = [StructField("address", StringType(), True),
                                       StructField("suburb", StringType(), True),
                                       StructField("state", StringType(), True),
                                       StructField("postcode", IntegerType(), True),                                       
                                       StructField("full_address", StringType(), True),
                                       StructField("price", StringType(), True),                                        
                                       StructField("sold_date", StringType(), True),
                                       StructField("type", StringType(), True),
                                       StructField("bedrooms", IntegerType(), True),
                                       StructField("bathrooms", IntegerType(), True),
                                       StructField("car_space", IntegerType(), True),
                                       StructField("land_size", IntegerType(), True),
                                       StructField("building_size", IntegerType(), True),
                                       StructField("built_date", StringType(), True),
                                       StructField("rent", StringType(), True),
                                       StructField("rent_date", StringType(), True),                                       
                                       StructField("agent", StringType(), True),
                                       StructField("lat", DoubleType(), True),
                                       StructField("lng", DoubleType(), True)                                       
])

# COMMAND ----------

df = spark.read \
    .option("header", True) \
    .schema(property_schema) \
    .csv(f"{raw_folder_path}/{v_file_date}/")

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2. Write data to datalake

# COMMAND ----------

df.write.mode('overwrite').format("delta").saveAsTable("pp1_raw.perth_bronze")

# COMMAND ----------

# MAGIC %sql
# MAGIC Select * from pp1_raw.perth_bronze;

# COMMAND ----------

