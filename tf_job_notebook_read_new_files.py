# Databricks notebook source
# Create widgets for volume_path and table_name
dbutils.widgets.text("volume_path", "/Volumes/users/sebastian/input_files/") 
dbutils.widgets.text("output_table", "users.sebastian.titanic_sample_data")

# Get the values from the widgets
volume_path = dbutils.widgets.get("volume_path")
table_name = dbutils.widgets.get("output_table")

# COMMAND ----------

from pyspark.sql.functions import col, current_timestamp

schema = "Survived INT, Pclass INT, Name STRING, Sex STRING, Age FLOAT, SibSp INT, Parch INT, Ticket STRING, Fare FLOAT, Cabin STRING, Embarked STRING"

input_path  = f"{volume_path}*.csv"
check_path  = f"{volume_path}checkpoint/"

(spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .schema(schema)
    .load(input_path)
    .select("*", col("_metadata.file_path").alias("source_file"), current_timestamp().alias("processing_time"))
  .writeStream
    .format("delta")
    .option("checkpointLocation", check_path)
    .trigger(availableNow = True)
    .outputMode("append")
    .toTable(table_name))
