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
# MAGIC ##### Step 7. Write data to datalake

# COMMAND ----------

CityOfPerth_df.write.mode('overwrite').format("delta").saveAsTable("pp1_processed.perth_silver")

# COMMAND ----------

dbutils.notebook.exit("Success")

# COMMAND ----------

perth_silver%sql 
Select * from pp1_processed.perth_silver

# COMMAND ----------

# MAGIC %sql 
# MAGIC select count(1) from pp1_processed.perth_silver

# COMMAND ----------

