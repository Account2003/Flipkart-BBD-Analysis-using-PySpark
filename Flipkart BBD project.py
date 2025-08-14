# Databricks notebook source

dbutils

# COMMAND ----------

dbutils.help()

# COMMAND ----------

dbutils.fs.help()

# COMMAND ----------

from pyspark.sql import SparkSession

# Create a SparkSession
spark = SparkSession.builder \
    .appName("CSVImportExample") \
    .getOrCreate()

# Define the path to your CSV file
csv_file_path = "/Workspace/Users/podderankit46@gmail.com/test.csv"

# Read the CSV file into a DataFrame
# header=True indicates that the first row is a header
# inferSchema=True attempts to automatically determine column data types
flipkart_df = spark.read.csv("dbfs:/FileStore/tables/test.csv", header=True, inferSchema=True)
flipkart_df.show(5)

# COMMAND ----------

# Load the CSV file
file_path = '/Workspace/Users/podderankit46@gmail.com/test.csv'
flipkart_df = spark.read.csv(file_path, header=True, inferSchema=True)
display(dbutils.fs.ls("/Workspace/Users/podderankit46@gmail.com/test.csv"))

# COMMAND ----------

# loading the csv files into the spark dataframe
flipkart_df =spark.sql("select * from workspace.default.test")
display(flipkart_df)


# COMMAND ----------

flipkart_df.printSchema()
flipkart_df.show(5)

# COMMAND ----------

#imports
from pyspark.sql.functions import col,isnan,when,count           
from pyspark.sql.functions import expr
from pyspark.sql.functions import *

# COMMAND ----------

# finding the missing values
flipkart_df.select([count(when(col(c).isNull(), c)).alias(c) for c in flipkart_df.columns]).display()
# droping the missing values
flipkart_df_clean = flipkart_df.dropna()
# filling specific values
flipkart_df_filled =flipkart_df.fillna({"Rating":0})

# COMMAND ----------

#Data transformation
flipkart_df_transformed = flipkart_df.withcolumn("EffectivePrice",expr("Price-(Price*Discount/100)"))
#Show the updated dataframe
flipkart_df_transformed.select("ProductName","Price","Discount","EffectivePrice").show(5)


# COMMAND ----------

# filter product with rating 4 and priced below 1000
high_rated_products = flipkart_df.filter((col("Rating") == 4))
# show the results
high_rated_products.display(5)

# COMMAND ----------

#group by category and calculate avg rating
avg_rating_by_category = flipkart_df_filled.groupBy("maincateg").agg({"Rating": "avg"})
#display the results
avg_rating_by_category.display()

# COMMAND ----------

# total revenue by category
total_revenue_by_category = flipkart_df_filled.groupBy("maincateg").agg(sum("Rating"))
total_revenue_by_category.display()

# COMMAND ----------

#save the data
output_table='Flipkart_Analysis'
flipkart_df_filled.write.mode("overwrite").saveAsTable(output_table)


# COMMAND ----------

# MAGIC %sql
# MAGIC select * from Flipkart_Analysis limit 20
# MAGIC

# COMMAND ----------

_sqldf.coalesce(1).write.mode("overwrite").option("header", "true").csv("/dbfs/FileStore/sqldf_output")
