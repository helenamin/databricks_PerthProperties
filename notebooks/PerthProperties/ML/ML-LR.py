# Databricks notebook source
# MAGIC %md
# MAGIC #### Read all data as required

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

df = spark.read.format("delta").load(f"{processed_folder_path}/perth_silver")

# COMMAND ----------

display(df)

# COMMAND ----------

df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Filter Outliers
# MAGIC * Properties sold before 2019
# MAGIC * landsize over 2000 sqm
# MAGIC * carspace, bedroom over 6
# MAGIC * properties price over 3M

# COMMAND ----------

Less2000sqm_df = df.filter("land_size <= 2000")

# COMMAND ----------

Less2000sqm_df.count()

# COMMAND ----------

carspaceLessThan7_df = Less2000sqm_df.filter("car_space < 7")

# COMMAND ----------

carspaceLessThan7_df.count()

# COMMAND ----------

BedroomsLessthan7_df = carspaceLessThan7_df.filter(" bedrooms < 7")

# COMMAND ----------

BedroomsLessthan7_df.count()

# COMMAND ----------

filtered_df = BedroomsLessthan7_df.filter("price < 3000000")

# COMMAND ----------

filtered_df.count()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Data Preporcessing

# COMMAND ----------

house_df = filtered_df.filter("property_type = 'House' or property_type = 'Villa' or property_type = 'Townhouse'")

# COMMAND ----------

house_df.count()

# COMMAND ----------

import pyspark.sql.functions as F

suburbs = house_df.select("suburb").distinct().rdd.flatMap(lambda x: x).collect()
property_types = house_df.select("property_type").distinct().rdd.flatMap(lambda x: x).collect()

suburb_expr = [F.when(F.col("suburb") == s, 1).otherwise(0).alias("suburb_" + s) for s in suburbs]
property_type_expr = [F.when(F.col("property_type") == p, 1).otherwise(0).alias("property_type_" + p) for p in property_types]
final_df = house_df.select('price','bathrooms', 'bedrooms', 'building_size',
       'built_year', 'car_space', 'land_size', 'sold_year', 'sold_month', *suburb_expr+property_type_expr)

# COMMAND ----------

display(final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Data exploration

# COMMAND ----------

final_df.printSchema()

# COMMAND ----------

final_df_toPandas = final_df.toPandas()
features_df = final_df_toPandas.drop('price', axis=1)
features_df

# COMMAND ----------

# MAGIC %md
# MAGIC #### Perform descriptive analytics

# COMMAND ----------

features_df.describe().transpose()

# COMMAND ----------

import pandas as pd
from pandas.plotting import scatter_matrix

numeric_features = [t[0] for t in features_df.dtypes if t[1] == 'int' or t[1] == 'double']
sampled_data = features_df.select(numeric_features).sample(False, 0.8).toPandas()
axs = scatter_matrix(sampled_data, figsize=(10, 10))
n = len(sampled_data.columns)
for i in range(n):
    v = axs[i, 0]
    v.yaxis.label.set_rotation(0)
    v.yaxis.label.set_ha('right')
    v.set_yticks(())
    h = axs[n-1, i]
    h.xaxis.label.set_rotation(90)
    h.set_xticks(())

# COMMAND ----------

import matplotlib.pyplot as plt
import seaborn  as sns
plt.figure(figsize=(12,10))
cor = features_df.corr()
sns.heatmap(cor, annot = True, cmap= plt.cm.CMRmap_r)

# COMMAND ----------

def correlation(dataset, threshold):
    col_corr = set()
    corr_matrix = dataset.corr()
    for i in range(len(corr_matrix.columns)):
        for j in range(i):
            if abs(corr_matrix.iloc[i,j]) > threshold:
                colname = corr_matrix.columns[i]
                col_corr.add(colname)
    return col_corr

# COMMAND ----------

corr_features = correlation(features_df, 0.9)
len(set(corr_features))

# COMMAND ----------

corr_features

# COMMAND ----------

# Assign the data to X and y
final_df_pandas = final_df.toPandas()
X = final_df_pandas[['bathrooms', 'bedrooms', 'building_size', 'built_year',\
              'car_space', 'land_size', 'sold_year', 'sold_month',  \
              'suburb_Crawley', 'suburb_East Perth', 'suburb_Nedlands', \
              'suburb_Northbridge', 'suburb_Perth', 'suburb_West Perth', \
              'property_type_House', 'property_type_Townhouse', \
              'property_type_Villa']]
y = final_df_pandas["price"].values.reshape(-1, 1)
print(X.shape, y.shape)

# COMMAND ----------

from sklearn.model_selection import train_test_split

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=50)

# COMMAND ----------

X_train.shape,X_test.shape

# COMMAND ----------

import matplotlib.pyplot as plt
import seaborn  as sns
plt.figure(figsize=(12,10))
cor = X_train.corr()
sns.heatmap(cor, annot = True, cmap= plt.cm.CMRmap_r)

# COMMAND ----------

features = X_train.columns
features

# COMMAND ----------

X_train = X_train.drop(corr_features, axis=1)

# COMMAND ----------

X_train

# COMMAND ----------

X_train.columns

# COMMAND ----------

X_train = X_train[['bathrooms', 'bedrooms', 'building_size', 'built_year', 'car_space',
       'land_size', 'sold_year','sold_month', 'suburb_Crawley', 'suburb_East Perth',
       'suburb_Nedlands', 'suburb_Northbridge', 'suburb_Perth', 'suburb_West Perth']]
X_train

# COMMAND ----------

X_test = X_test[['bathrooms', 'bedrooms', 'building_size', 'built_year', 'car_space',
       'land_size', 'sold_year','sold_month', 'suburb_Crawley', 'suburb_East Perth',
       'suburb_Nedlands', 'suburb_Northbridge', 'suburb_Perth', 'suburb_West Perth']]
X_test

# COMMAND ----------

from sklearn.preprocessing import StandardScaler

# Create a StandardScater model and fit it to the training data

X_scaler = StandardScaler().fit(X_train)
y_scaler = StandardScaler().fit(y_train)

# COMMAND ----------

# Transform the training and testing data using the X_scaler and y_scaler models

X_train_scaled = X_scaler.transform(X_train)
X_test_scaled = X_scaler.transform(X_test)
y_train_scaled = y_scaler.transform(y_train)
y_test_scaled = y_scaler.transform(y_test)

# COMMAND ----------

X_train_scaled

# COMMAND ----------

# Create the model using LinearRegression

from pyspark.ml.regression import LinearRegression
lin_reg = LinearRegression(featuresCol = 'features', labelCol='price')
model= lin_reg.fit(final_df_pandas)

# COMMAND ----------

