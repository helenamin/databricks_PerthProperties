# Databricks notebook source
# MAGIC %run "../includes/configuration"

# COMMAND ----------

# MAGIC %run "../includes/common_functions"

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Step 1. Load Perth_silver table 

# COMMAND ----------

from pyspark.sql.functions import col
df = spark.sql("Select * from pp1_processed.perth_silver where sold_year > 2019")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Average price per suburb since 2020

# COMMAND ----------

from pyspark.sql.functions import sum, avg, count, when, round
avg_price_df = df.groupBy("sold_year","suburb") \
.agg(avg("price").alias("avg_price"))

# COMMAND ----------

avg_price_df = avg_price_df.withColumn("avg_price", round("avg_price", 0))

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql.functions import desc, rank

expensiveSuburbRankSpec = Window.partitionBy("sold_year").orderBy(desc("avg_price"))
final_df = avg_price_df.withColumn("rank" , rank().over(expensiveSuburbRankSpec))

# COMMAND ----------

display(final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Write the final_df

# COMMAND ----------

final_df.write.mode("overwrite").format("delta").saveAsTable("pp1_presentation.expensiveSuburbs_gold")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from pp1_presentation.expensiveSuburbs_gold where sold_year =2020;

# COMMAND ----------

