# Databricks - Perth City Properties

## Table of Contents

- [Introduction](#Introduction)
- [Prerequisites](#Prerequisites)
- [Azure-Storage-Solutions](#Azure-Storage-Solutions)
- [Data-Orchestration](#Data-Orchestration)
- [Databricks](#Databricks)
- [DeltaLake](#DeltaLake)
- [mlflow](#mlflow)
- [Data Sources](#DataSources)
- [Technology](#Technology)
- [Contributors](#Contributors)

## Introduction

<b>Project Outline:</b>

A Re-do of Perth City Properties project using Azure Data Engineering technologies such as Azure Data Factory (ADF), Azure Data Lake Storage Gen2, Azure Blob Storage, Azure Databricks.

In this project I'd like to:

* Add Data orchestration using Azure Data Factory
* perform data ingestion and transformation on the dataset using databricks
* Implement ML models on databricks Machine learning and keep track of changes in ML notebooks and models use mlflow. Then  register the best model using MLflow Model Registry

## Prerequisites

An Azure subscription


## Azure-Storage-Solutions

Creating Azure Data Lake Gen2 and containers

   ![Azure Data Lake Gen2](static/images/1.png)

Using Azure Storage explorer to interact with the storage account

   ![Storage explorer](static/images/2.png)

Uploading data into raw folder

   ![Uploading data into raw folder](static/images/3.png)

Access Control (IAM) role assignment

   ![IAM - Storage](static/images/4.png)




## Data-Orchestration

Integrating data from Azure Data Lake Gen2 using Azure Data Factory.

   ![Factory Resource Dataset](static/images/5.png)

Creating dependency between pipelines to orchestrate the data flow
* I've created 3 pipelines. 
    1. One of them is to run the ingestion databricks notebook to get raw data and create bronze table and then ingest them into silver table.
    2. One to create gold table.
    3. This one to orchestratte the previous two pipeline. Using this I make sure To do the ingestion first and then Transformation

   ![Pipeline dependencies](static/images/6.png) 

Branching and Chaining activities in Azure Data Factory (ADF) Pipelines using control flow activities such as Get Metadata. If Condition, ForEach, Delete, Validation etc.

   ![Branching and Chaining](static/images/7.png) 

Using Parameters and Variables in Pipelines, Datasets and LinkedServices to create a metadata driven pipelines in Azure Data Factory (ADF)
   
   ![link service](static/images/8.png) 

   ![parameters](static/images/9.png) 

Debugging the data pipelines and resolving issues.

   ![Debug](static/images/10.png) 

Scheduling pipelines using trigger - Tumbling Window Trigger(for past time dataset) in Azure Data Factory (ADF)

   ![Trigger](static/images/11.png) 

Creating ADF pipelines to execute Databricks Notebook activities to carry out transformations.

   ![ADF - transformation](static/images/12.png)

enable ADF git integration

   ![ADF - git integration](static/images/13.png)


## Databricks

Creating Azure Databricks Workspace

   ![Databricks Workspace2](static/images/14.png)

   ![Databricks Workspace2](static/images/15.png)

Creating Databricks cluster
   
   ![Databricks cluster](static/images/16.png)

Mounting storage accounts using Azure Key Vault and Databricks Secret scopes

   ![Mounting](static/images/17.png)

Creating Databricks notebooks

   ![notebooks](static/images/18.png)

performing transformations using Databricks notebooks

   ![ingestion](static/images/19.png)

enable databricks git integration

   ![git - databricks](static/images/20.png)


## DeltaLake

I've build a pipeline that runs databrick notebooks to reads data into Delta tables. Using the function below, it checks if the data needs to be merged or inserted.

   ![deltaTable - merge](static/images/25.png)


In this project, the pipeline reads the data from raw folder in perthpropdl (Azure Data Lake Gen2 storage) and creates a delta perth_bronze table. 
Then the bronze table is used in ingestion notebook to create perth_silver table. And finally gold tables are created using perth_silver table.
IN this project, I didnt focus on gold table and data visulization after that. I just wanted to show the way it can be created in pipeline.

Source -> Bronze 

   ![bronze](static/images/21.png)

Bronze -> Silver

   ![silver](static/images/22.png)

Silver -> Gold 

   ![delta- gold](static/images/24.png)

   ![gold](static/images/23.png)

## mlflow
MLflow is an open source platform for managing the end-to-end machine learning lifecycle. In this project I've used these models to predict Perth Property price ranges:
* Linear Regression
* Lasso 
* Ridge
* Elasticnet

Again the main purpose of this project was to show how to use mlflow. So there will be room to improve the models.

##### Linear Regression

   ![LR](static/images/27.png)


##### Lasso
   
   ![Lasso](static/images/28.png)

   ![Lasso - best alpha](static/images/29.png)


##### Ridge

   ![Ridge](static/images/30.png)

   ![Ridge - best alpha](static/images/31.png)
   

##### Elasticnet

   ![Elasticnet](static/images/32.png)

   ![Elasticnet - best alpha](static/images/33.png)
   

### MLflow Model Registry 
It is a centralized model repository and I used it to register the best model base on current r2 in experiment UI:

   ![experiment UI](static/images/34.png)   

   ![Model Registry](static/images/26.png)

## DataSources
http://house.speakingsame.com/ 

https://www.onthehouse.com.au/

https://www.propertyvalue.com.au/


## Technology

![PythonLogo](static/images/tools.png)

## Contributors

- [Helen Amin](https://github.com/helenamin)
