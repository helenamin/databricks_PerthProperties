# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1. Load Perth_bronze table 

# COMMAND ----------

df = spark.sql("Select * from pp1_raw.perth_bronze")

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 2. Remove unwanted characters from columns

# COMMAND ----------

from pyspark.sql.functions import regexp_replace
df = df.withColumn("price", regexp_replace("price", "[$,]", "")) \
.withColumn("rent", regexp_replace("rent", "[$,]", "")) \
.withColumn("rent", regexp_replace("rent", "pw", "")) \
.withColumn("rent_date", regexp_replace("rent_date", "1/01/1900", "0/00/0000"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 3. Split date columns to required columns

# COMMAND ----------

from pyspark.sql.functions import substring
df = df.withColumn("sold_month", substring("sold_date", 3, 2)).withColumn("sold_year", substring("sold_date", 6, 4)) \
.withColumn("rent_month", substring("rent_date", 3, 2)).withColumn("rent_year", substring("rent_date", 6, 4)) \
.withColumn("built_year", substring("built_date", 6, 4))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 4. Remove rows with null values

# COMMAND ----------

df = df.dropna()

# COMMAND ----------

df.dtypes

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 5. Select only required columns

# COMMAND ----------

from pyspark.sql.functions import col
selected_df = df.select(col("address"), col("suburb"), col("state"), col("postcode"), \
                        col("full_address"), col("sold_year").cast("int"), col("sold_month").cast("int"), \
                        col("price").cast("int"), col("type"), col("bedrooms"), col("bathrooms"),\
                        col("car_space"), col("built_year").cast("int"), col("building_size"), \
                        col("land_size"), col("rent_year").cast("int"), col("rent_month").cast("int"),\
                        col("rent").cast("int"), col("agent"), col("lat"), col("lng"))

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 6. Rename the columns as required

# COMMAND ----------

CityOfPerth_df = selected_df.withColumnRenamed('type', 'property_type')

# COMMAND ----------

CityOfPerth_df.dtypes

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 7. Write data to datalake (Create perth_silver table in pp1_processed database)

# COMMAND ----------

CityOfPerth_df.write.mode('overwrite').format("delta").saveAsTable("pp1_processed.perth_silver")

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

# MAGIC %sql 
# MAGIC Select * from pp1_processed.perth_silver

# COMMAND ----------

# MAGIC %sql 
# MAGIC select count(1) from pp1_processed.perth_silver

# COMMAND ----------

