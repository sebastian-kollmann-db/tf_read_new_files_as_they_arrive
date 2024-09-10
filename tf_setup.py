# Databricks notebook source
# DBTITLE 1,set all necessary input variables
# adapt the variables
catalog_name  = "users"
schema_name   = "sebastian"
volume_name   = "input_files"
table_name    = "titanic_sample_data"
# ID of the existing cluster in the workspace to run the job on
existing_cluster_id = "5910-093655-5qagthnr"

# adapt path to notebook if not correct
volume_path   = f"/Volumes/{catalog_name}/{schema_name}/{volume_name}/"
table_fq_name = f"{catalog_name}.{schema_name}.{table_name}"
current_user  = spark.sql("SELECT current_user()").collect()[0][0]
notebook_path = f"/Workspace/Users/{current_user}/tf_job_notebook_read_new_files"

# COMMAND ----------

# DBTITLE 1,create schema/volume
spark.sql(f"USE CATALOG {catalog_name}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_name}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog_name}.{schema_name}.{volume_name}")

# COMMAND ----------

# DBTITLE 1,function to get data from the internet
import requests
import os
from datetime import datetime

def download_and_save_titanic_data():
    # URL of the Titanic dataset
    url = "https://web.stanford.edu/class/archive/cs/cs109/cs109.1166/stuff/titanic.csv"

    # Download the dataset
    response = requests.get(url)
    data = response.text

    # Generate the current timestamp
    current_timestamp = datetime.now().strftime("%Y%m%d%H%M%S")

    # Save the dataset to the volume
    file_path = f"{volume_path}titanic_downloaded_{current_timestamp}.csv"
    with open(file_path, "w") as file:
        file.write(data)

    return file_path

# COMMAND ----------

# DBTITLE 1,create a job reusing an existing cluster
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import Task, NotebookTask, Source, TriggerSettings, FileArrivalTriggerConfiguration, PauseStatus

w = WorkspaceClient()

j = w.jobs.create(
  name = "tf: job triggered on new file arrival",
  tasks = [
    Task(
      existing_cluster_id = existing_cluster_id,
      notebook_task = NotebookTask(
        notebook_path = notebook_path,
        base_parameters = {
          "volume_path": volume_path,
          "output_table": table_fq_name
        },
        source = Source("WORKSPACE")
      ),
      task_key = "auto_load_data"
    )
  ],
  trigger = TriggerSettings(
    file_arrival = FileArrivalTriggerConfiguration(
      url = volume_path
    ),
    pause_status = PauseStatus(
      "UNPAUSED"
    )
  )
)

print(f"Job was created: {j.job_id}")

# COMMAND ----------

# MAGIC %md
# MAGIC Execute the following two cells alternating (and wait a bit in between):
# MAGIC
# MAGIC 1. with `download_and_save_titanic_data()` you can add a new file to the volume
# MAGIC 1. with `SELECT COUNT(*) AS cnt, COUNT(*)/888 AS number_of_files 
# MAGIC FROM {catalog_name}.{schema_name}.{table_name}` you can easily check if new data arrived in the table (ergo: if the job ran successful)

# COMMAND ----------

# DBTITLE 1,get new data and put it in the volume
print("New data arrived in: " + download_and_save_titanic_data())

# COMMAND ----------

# DBTITLE 1,check if new data arrived
# divided by 888 as there are 888 entries in the csv
query = f"SELECT COUNT(*) AS cnt, COUNT(*)/888 AS number_of_files FROM {catalog_name}.{schema_name}.{table_name}"

df = spark.sql(query)
display(df)

# COMMAND ----------

# DBTITLE 1,see which source files have been processed
display(
  spark.sql(f"SELECT distinct source_file FROM {catalog_name}.{schema_name}.{table_name}")
)
