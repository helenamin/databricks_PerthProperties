# Databricks notebook source
# MAGIC %md
# MAGIC #### Read all data as required

# COMMAND ----------

# MAGIC %run "../includes/configuration"

# COMMAND ----------

df = spark.read.format("delta").load(f"{silver_folder_path}/perth_silver")

# COMMAND ----------

display(df)

# COMMAND ----------

df.count()

# COMMAND ----------

display(df.summary())

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Filter Outliers
# MAGIC * landsize over 2000 sqm
# MAGIC * carspace, bedroom over 6
# MAGIC * Building size 2sqm or less and over 500 sqm
# MAGIC * properties price over 3M

# COMMAND ----------

LandLess2000sqm_df = df.filter("land_size <= 2000")

# COMMAND ----------

LandLess2000sqm_df.count()

# COMMAND ----------

Buildingfiltered_df = LandLess2000sqm_df.filter("building_size <= 500 and building_size > 2")

# COMMAND ----------

Buildingfiltered_df.count()

# COMMAND ----------

carspaceLessThan7_df = Buildingfiltered_df.filter("car_space < 7")

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

def absolute(number):
    return number*10.0

house_df = house_df.withColumn('lat', absolute(house_df.lat))


# COMMAND ----------

display(house_df.summary())

# COMMAND ----------

import pyspark.sql.functions as F

suburbs = house_df.select("suburb").distinct().rdd.flatMap(lambda x: x).collect()
property_types = house_df.select("property_type").distinct().rdd.flatMap(lambda x: x).collect()

suburb_expr = [F.when(F.col("suburb") == s, 1).otherwise(0).alias("suburb_" + s) for s in suburbs]
property_type_expr = [F.when(F.col("property_type") == p, 1).otherwise(0).alias("property_type_" + p) for p in property_types]
final_df = house_df.select('price','bathrooms', 'bedrooms', 'building_size',
       'built_year', 'car_space', 'land_size', 'sold_year', 'sold_month', 'lat', 'lng', *suburb_expr+property_type_expr)

# COMMAND ----------

display(final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC #### Data exploration

# COMMAND ----------

final_df.printSchema()

# COMMAND ----------

final_df_toPandas = final_df.toPandas()

# COMMAND ----------

features_df = final_df_toPandas.drop('price', axis=1)
features_df

# COMMAND ----------

# MAGIC %md
# MAGIC #### Perform descriptive analytics

# COMMAND ----------

features_df.describe().transpose()

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

features_df.columns

# COMMAND ----------

corr_features

# COMMAND ----------

features_df = features_df.drop(corr_features, axis=1)

# COMMAND ----------

features_df.columns

# COMMAND ----------

# MAGIC %md
# MAGIC #### As we only have 3 villas in dataset, decided not to use Property type

# COMMAND ----------

features_df = features_df.drop('property_type_Townhouse', axis=1)
features_df = features_df.drop('property_type_Villa', axis=1)

# COMMAND ----------

features_df.columns

# COMMAND ----------

# MAGIC %md
# MAGIC ### Final Dataset

# COMMAND ----------

final_df = final_df[['price','bathrooms', 'bedrooms', 'building_size', 'built_year', 'car_space',
       'land_size', 'sold_year', 'sold_month', 'lat', 'lng',
       'suburb_West Perth', 'suburb_Perth', 'suburb_Crawley',
       'suburb_Northbridge', 'suburb_East Perth', 'suburb_Nedlands']]

# COMMAND ----------

display(final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC # Track Runs

# COMMAND ----------

#dependencies
import mlflow
import numpy as np
import pandas as pd
from sklearn.linear_model import Lasso, Ridge, ElasticNet
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
from sklearn.model_selection import GridSearchCV
from sklearn.model_selection import train_test_split
import mlflow.spark
from pyspark.ml.regression import LinearRegression
from pyspark.ml.feature import VectorAssembler
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator

# COMMAND ----------

# MAGIC %md
# MAGIC ### Linear Regression model

# COMMAND ----------

# MAGIC %md
# MAGIC #### Train/Test Split

# COMMAND ----------

trainDF, testDF = final_df.randomSplit([.7,.3], seed = 42)
print(trainDF.cache().count())

# COMMAND ----------

with mlflow.start_run(run_name ="LinearRegression") as run:
    #Define Pipeline
    vecAssembler = VectorAssembler(inputCols=['bathrooms', 'bedrooms', 'building_size', 'built_year', 'car_space', \
                                               'land_size', 'sold_year', 'sold_month', 'lat', 'lng', \
                                               'suburb_West Perth', 'suburb_Perth', 'suburb_Crawley', \
                                               'suburb_Northbridge', 'suburb_East Perth', 'suburb_Nedlands'], outputCol="features")
    lr = LinearRegression(featuresCol="features", labelCol="price")
    pipeline = Pipeline(stages=[vecAssembler,lr])
    pipelineModel = pipeline.fit(trainDF)
    
    # log parameters
    mlflow.log_param("label", "LinearRegression")
    
    #log model
    mlflow.spark.log_model(pipelineModel, "model")
    
    #Evaluate predictions
    predDF = pipelineModel.transform(testDF)
    regressionEvaluator = RegressionEvaluator(predictionCol="prediction", labelCol="price", metricName = "rmse")
    rmse = regressionEvaluator.evaluate(predDF)
    regressionEvaluator = RegressionEvaluator(predictionCol="prediction", labelCol="price", metricName = "r2")
    r2 = regressionEvaluator.evaluate(predDF)
    
    #log metrics
    mlflow.log_metric("RMSE", rmse)
    mlflow.log_metric("r2", r2)

# COMMAND ----------

# MAGIC %md
# MAGIC ### LASSO model

# COMMAND ----------

# MAGIC %md
# MAGIC #### Train/Test Split

# COMMAND ----------

## Split selected_df to training and testing
# Specify the independent variables (X) and dependent variable (y)
X = final_df.toPandas().drop('price', axis=1)
y = final_df.toPandas()['price']

# Split data to testing&training
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3, random_state=42)

# COMMAND ----------

# end the one above to try the ridge experiment
mlflow.end_run()

# Ridge experiment
with mlflow.start_run(run_name = "Lasso - alpha 100 - Experiment") as run:
  # Run LinearRegression model
  lasso = Lasso(alpha = 100, normalize = True)
  
  # Naive model
  lasso_model = lasso.fit(X_train, y_train)
  lasso_pred = lasso.predict(X_test)
  
  # Log model
  mlflow.sklearn.log_model(lasso_model, "lasso-model- alpha 100")

  # log parameters
  mlflow.log_param("label", "Lasso - alpha 100")
  
  # Metrics
  rmse = np.sqrt(mean_squared_error(y_test, lasso_pred))
  print("Root Mean Squared Error: {}". format(rmse))
  mae = mean_absolute_error(y_test, lasso_pred)
  r2 = r2_score(y_test, lasso_pred)
  print("R_squared: {}". format(r2))
  
  # Log metrics
  mlflow.log_metric("RMSE", rmse)
  mlflow.log_metric("r2", r2)
  
  runID = run.info.run_uuid
  experimentID = run.info.experiment_id
  
  print("Inside MLflow Run with run_id {}, experiment_id{}".format(runID, experimentID))

# COMMAND ----------


# find optimal alpha with grid search
alpha = [0.001, 0.01, 0.1, 1, 10, 100, 1000]

param_grid = dict(alpha=alpha)

lasso_grid = GridSearchCV(estimator=lasso, param_grid=param_grid, scoring='r2', verbose=1, n_jobs=-1)

lasso_grid_result = lasso_grid.fit(X_train, y_train)

# lasso_grid_predictions = lasso_grid_model(X_test_scaled)

# MSE = mean_squared_error(y_test_scaled, lasso_grid_predictions) #error to a model (closer to 0 the better)
# r2 = lasso_grid_model.score(X_test_scaled, y_test_scaled) #nearer to 1 the better

# print(f"MSE: {MSE}, R2: {r2}")
print('Best Score: ', lasso_grid_result.best_score_)
print('Best Params: ', lasso_grid_result.best_params_)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ridge model

# COMMAND ----------

# end the one above to try the ridge experiment
mlflow.end_run()

# Ridge experiment
with mlflow.start_run(run_name = "Rigde - alpha 0.1 - Experiment") as run:
  # Run LinearRegression model
  ridge = Ridge(alpha = 0.1, normalize = True)
  
  # Naive model
  ridge_model = ridge.fit(X_train, y_train)
  ridge_pred = ridge.predict(X_test)
  
  # Log model
  mlflow.sklearn.log_model(ridge_model, "ridge-model -alpha 0.1")

  # log parameters
  mlflow.log_param("label", "ridge - alpha 0.1")
  
  # Metrics
  rmse = np.sqrt(mean_squared_error(y_test, ridge_pred))
  print("Root Mean Squared Error: {}". format(rmse))
  mae = mean_absolute_error(y_test, ridge_pred)
  r2 = r2_score(y_test, ridge_pred)
  print("R_squared: {}". format(r2))
  
  # Log metrics
  mlflow.log_metric("RMSE", rmse)
  mlflow.log_metric("r2", r2)
  
  runID = run.info.run_uuid
  experimentID = run.info.experiment_id
  
  print("Inside MLflow Run with run_id {}, experiment_id{}".format(runID, experimentID))

# COMMAND ----------

# find optimal alpha with grid search
alpha = [0.001, 0.01, 0.1, 1, 10, 100, 1000]

param_grid = dict(alpha=alpha)

ridge_grid = GridSearchCV(estimator=ridge, param_grid=param_grid, scoring='r2', verbose=1, n_jobs=-1)

ridge_grid_result = ridge_grid.fit(X_train, y_train)

# ridge_grid_predictions = ridge_grid(X_test_scaled)

# MSE = mean_squared_error(y_test_scaled, ridge_grid_predictions) #error to a model (closer to 0 the better)
# r2 = ridge_grid.score(X_test_scaled, y_test_scaled) #nearer to 1 the better

# print(f"MSE: {MSE}, R2: {r2}")
print('Best Score: ', ridge_grid_result.best_score_)
print('Best Params: ', ridge_grid_result.best_params_)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Elasticnet model

# COMMAND ----------

# end the one above to try the ridge experiment
mlflow.end_run()

# Ridge experiment
with mlflow.start_run(run_name = "Elasticnet - alpha 0.001 - Experiment") as run:
  # Run LinearRegression model
  elasticnet = ElasticNet(alpha = .001, normalize = True)
  
  # Naive model
  elasticnet_model = elasticnet.fit(X_train, y_train)
  elasticnet_pred = elasticnet.predict(X_test)
  
  # Log model
  mlflow.sklearn.log_model(elasticnet_model, "elasticnet-model - alpha 0.001")

  # log parameters
  mlflow.log_param("label", "elasticnet - alpha 0.001")
  
  # Metrics
  rmse = np.sqrt(mean_squared_error(y_test, elasticnet_pred))
  print("Root Mean Squared Error: {}". format(rmse))
  mae = mean_absolute_error(y_test, elasticnet_pred)
  r2 = r2_score(y_test, elasticnet_pred)
  print("R_squared: {}". format(r2))
  
  # Log metrics
  mlflow.log_metric("RMSE", rmse)
  mlflow.log_metric("r2", r2)
  
  runID = run.info.run_uuid
  experimentID = run.info.experiment_id
  
  print("Inside MLflow Run with run_id {}, experiment_id{}".format(runID, experimentID))

# COMMAND ----------

# find optimal alpha with grid search
alpha = [0.001, 0.01, 0.1, 1, 10, 100, 1000]

param_grid = dict(alpha=alpha)

elasticnet_grid = GridSearchCV(estimator=elasticnet, param_grid=param_grid, scoring='r2', verbose=1, n_jobs=-1)

elasticnet_grid_result= elasticnet_grid.fit(X_train, y_train)

# elasticnet_grid_predictions = elasticnet_grid(X_test_scaled)

# MSE = mean_squared_error(y_test_scaled, elasticnet_grid_predictions) #error to a model (closer to 0 the better)
# r2 = elasticnet_grid.score(X_test_scaled, y_test_scaled) #nearer to 1 the better

# print(f"MSE: {MSE}, R2: {r2}")
print('Best Score: ', elasticnet_grid_result.best_score_)
print('Best Params: ', elasticnet_grid_result.best_params_)