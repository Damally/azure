// Databricks notebook source
// MAGIC %md 
// MAGIC # Demonstrate Databricks Delta using Flight On-time Performance Data
// MAGIC 
// MAGIC #### 1. Batch writes
// MAGIC #### 2. Stream writes
// MAGIC #### 3. Simulate write transaction failures
// MAGIC #### 4. Access delta via `SQL` and Data lineage
// MAGIC #### 5. Directories and Files
// MAGIC #### 6. Persist to SQLDW

// COMMAND ----------

// MAGIC %md ##Setup

// COMMAND ----------

// MAGIC %md #### Mount blob storage using SAS token - use secrets
// MAGIC * [Mount Azure Storage Docs](https://docs.azuredatabricks.net/spark/latest/data-sources/azure/azure-storage.html#mount-an-azure-blob-storage-container)
// MAGIC * [Secret Management Docs](https://docs.azuredatabricks.net/user-guide/secrets/index.html)
// MAGIC   * `databricks secrets create-scope --scope adb_events_demo_blob --profile adbdemo`
// MAGIC   * `databricks secrets write --scope adb_events_demo_blob --key sas_token --profile adbdemo`

// COMMAND ----------

dbutils.fs.mount(
  source = "wasbs://flight-data@adbeventsdemo.blob.core.windows.net/",
  mountPoint = "/mnt/adb-events-flights",
  extraConfigs = Map("fs.azure.sas.flight-data.adbeventsdemo.blob.core.windows.net" -> dbutils.preview.secret.get("adb_events_demo_blob", "sas_token"))
)

// COMMAND ----------

dbutils.fs.rm("/mnt/adb-events-delta/", true)

// COMMAND ----------

// MAGIC %sql 
// MAGIC 
// MAGIC DROP TABLE IF EXISTS adb_events_flights;
// MAGIC DROP TABLE IF EXISTS adb_events_airlines;
// MAGIC 
// MAGIC CREATE TABLE adb_events_flights(
// MAGIC   airline_code STRING,
// MAGIC   fl_num STRING,
// MAGIC   origin_airport_code STRING,
// MAGIC   dest_airport_code STRING,
// MAGIC   flight_date DATE,
// MAGIC   year INTEGER,
// MAGIC   month INTEGER,
// MAGIC   cancelled INTEGER,
// MAGIC   diverted INTEGER
// MAGIC )
// MAGIC USING DELTA
// MAGIC LOCATION "/mnt/adb-events-delta/landing";

// COMMAND ----------

// MAGIC %md 
// MAGIC ## 1. Batch writes:
// MAGIC ### Write flight events for first 3 months of 2017

// COMMAND ----------

val firstDf = spark.read.parquet("/mnt/adb-events-flights/year=2017/month=1",
                             "/mnt/adb-events-flights/year=2017/month=2",
                             "/mnt/adb-events-flights/year=2017/month=3")

firstDf
  .write
  .format("delta")
  .mode("append")
  .save("/mnt/adb-events-delta/landing")

// COMMAND ----------

// MAGIC %md ### Start a streaming query to count flights by month

// COMMAND ----------

// MAGIC %python
// MAGIC 
// MAGIC from pyspark.sql import functions as F
// MAGIC 
// MAGIC streamingDf = spark.readStream.format("delta").load("/mnt/adb-events-delta/landing")
// MAGIC streamingPartDF = streamingDf.select(F.month('flight_date').alias('month'), "cancelled")
// MAGIC 
// MAGIC display(
// MAGIC   streamingPartDF
// MAGIC   .groupby("month", "cancelled")
// MAGIC   .count()
// MAGIC   .orderBy("month")
// MAGIC )

// COMMAND ----------

// MAGIC %md 
// MAGIC ###Append flight events for next 3 months of 2017

// COMMAND ----------

val secondDf = spark.read.parquet("/mnt/adb-events-flights/year=2017/month=4",
                                 "/mnt/adb-events-flights/year=2017/month=5",
                                 "/mnt/adb-events-flights/year=2017/month=6")

secondDf
  .write
  .format("delta")
  .mode("append")
  .save("/mnt/adb-events-delta/landing")

// COMMAND ----------

// MAGIC %md ###...wait for streaming counts to show months `4`, `5` and `6`...

// COMMAND ----------

// MAGIC %md 
// MAGIC ## 2. Stream writes
// MAGIC ### Write streaming flight events for next 3 months of 2017

// COMMAND ----------

val thirdDf = spark.read.parquet("/mnt/adb-events-flights/year=2017/month=7",
                                 "/mnt/adb-events-flights/year=2017/month=8",
                                 "/mnt/adb-events-flights/year=2017/month=9")
val schema = thirdDf.schema

thirdDf.write
  .format("json")
  .mode("overwrite")
  .save("/mnt/adb-events-delta/eventsStream")

// COMMAND ----------

val simulatedStreamDf = spark
  .readStream
  .schema(schema)
  .option("maxFilesPerTrigger", 1)
  .json("/mnt/adb-events-delta/eventsStream")

simulatedStreamDf
  .writeStream
  .format("delta")
  .option("path", "/mnt/adb-events-delta/landing")
  .option("checkpointLocation", "/mnt/adb-events-delta/checkpoint/")
  .start()

// COMMAND ----------

// MAGIC %md ###...wait for streaming counts to show months `7`, `8` and `9`...

// COMMAND ----------

// MAGIC %md 
// MAGIC ## 3. Simulate write transaction failure
// MAGIC 
// MAGIC ### Try to append flight events with *bad* date format
// MAGIC ### Append flight events for last 3 months of 2017

// COMMAND ----------

// MAGIC %python
// MAGIC 
// MAGIC fourthDf = spark.read.parquet("/mnt/adb-events-flights/year=2017/month=10",
// MAGIC                               "/mnt/adb-events-flights/year=2017/month=11",
// MAGIC                               "/mnt/adb-events-flights/year=2017/month=12")
// MAGIC badDf = fourthDf.select("airline_code","fl_num","origin_airport_code","dest_airport_code","cancelled","diverted",F.date_format("flight_date","yyyy-MM-dd").alias("flight_date"))

// COMMAND ----------

// MAGIC %md 
// MAGIC ### Failure

// COMMAND ----------

// MAGIC %python
// MAGIC 
// MAGIC badDf \
// MAGIC   .write \
// MAGIC   .format("delta") \
// MAGIC   .mode("append") \
// MAGIC   .save("/mnt/adb-events-delta/landing")

// COMMAND ----------

// MAGIC %md 
// MAGIC ### Success!!

// COMMAND ----------

// MAGIC %python
// MAGIC 
// MAGIC fourthDf \
// MAGIC   .write \
// MAGIC   .format("delta") \
// MAGIC   .mode("append") \
// MAGIC   .save("/mnt/adb-events-delta/landing")

// COMMAND ----------

// MAGIC %md ###...wait for streaming counts to show months `10`, `11` and `12`...

// COMMAND ----------

// MAGIC %md 
// MAGIC ## 4. Access delta via `SQL` and Data lineage

// COMMAND ----------

// MAGIC %md #### Using delta file path

// COMMAND ----------

// MAGIC %sql 
// MAGIC 
// MAGIC SELECT count(*), month, cancelled
// MAGIC FROM
// MAGIC (SELECT month(flight_date) AS month, cancelled FROM delta.`/mnt/adb-events-delta/landing`) tmp
// MAGIC GROUP BY month, cancelled ORDER BY month ASC

// COMMAND ----------

// MAGIC %md #### Using delta table name

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC SELECT count(*), month, cancelled
// MAGIC FROM
// MAGIC (SELECT month(flight_date) AS month, cancelled FROM adb_events_flights) tmp
// MAGIC GROUP BY month, cancelled ORDER BY month ASC

// COMMAND ----------

// MAGIC %md #### Data lineage - Detail and History

// COMMAND ----------

// MAGIC %sql DESCRIBE DETAIL adb_events_flights

// COMMAND ----------

// MAGIC %sql DESCRIBE HISTORY adb_events_flights

// COMMAND ----------

// MAGIC %md #### File Compaction (Bin-packing)

// COMMAND ----------

// MAGIC %sql OPTIMIZE '/mnt/adb-events-delta/landing'

// COMMAND ----------

// MAGIC %md #### Rerun `Describe Detail` to see impact of optimize on file info

// COMMAND ----------

// MAGIC %sql DESCRIBE DETAIL adb_events_flights

// COMMAND ----------

// MAGIC %md 
// MAGIC ## 5. Directories and Files

// COMMAND ----------

// MAGIC %fs ls /mnt/adb-events-delta/landing

// COMMAND ----------

display(spark.read.parquet("/mnt/adb-events-delta/landing/part-00000-6ad3d01b-f044-48df-89a4-3d40fe0f945d-c000.snappy.parquet"))

// COMMAND ----------

// MAGIC %fs ls /mnt/adb-events-delta/landing/_delta_log/

// COMMAND ----------

// MAGIC %fs head /mnt/adb-events-delta/landing/_delta_log/00000000000000000000.json

// COMMAND ----------

// MAGIC %md 
// MAGIC ## 6. Persist to SQLDW

// COMMAND ----------

// MAGIC %md #### Setup to persist in SQL DW
// MAGIC * [Create the master key in SQL DW](https://docs.microsoft.com/en-us/sql/t-sql/statements/create-master-key-transact-sql?view=sql-server-2017) (if not created yet)
// MAGIC * We use the [SQL DW Connector](https://docs.azuredatabricks.net/spark/latest/data-sources/azure/sql-data-warehouse.html) here, that uses Blob Storage as intermediate storage with Polybase
// MAGIC * Setup secrets for intermediate blob storage and SQL DW
// MAGIC   * `databricks secrets write --scope adb_events_demo_blob --key account_key --profile adbdemo`
// MAGIC   * `databricks secrets write --scope adb_events_demo_dw --key username --profile adbdemo`
// MAGIC   * `databricks secrets write --scope adb_events_demo_dw --key password --profile adbdemo`

// COMMAND ----------

// MAGIC %sql CREATE DATABASE IF NOT EXISTS ADB_EVENTS_FLIGHTS_DW

// COMMAND ----------

// MAGIC %sql USE ADB_EVENTS_FLIGHTS_DW

// COMMAND ----------

spark.conf.set(
  "fs.azure.account.key.adbeventsdemo.blob.core.windows.net",
  dbutils.preview.secret.get("adb_events_demo_blob", "account_key")
)

// COMMAND ----------

// MAGIC %python
// MAGIC 
// MAGIC jdbcUrl = "jdbc:sqlserver://adbeventsdemodw.database.windows.net:1433;database=adbeventsdemodw;user=" + dbutils.preview.secret.get("adb_events_demo_dw", "username") + "@adbeventsdemodw;password=" + dbutils.preview.secret.get("adb_events_demo_dw", "password") + ";encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;"

// COMMAND ----------

// MAGIC %md #### Get data from Delta table to store in SQL DW

// COMMAND ----------

// MAGIC %python
// MAGIC 
// MAGIC flightsDataForDw = spark.sql("select * from default.adb_events_flights")

// COMMAND ----------

// MAGIC %python
// MAGIC 
// MAGIC flightsDataForDw.write \
// MAGIC   .mode("overwrite") \
// MAGIC   .format("com.databricks.spark.sqldw") \
// MAGIC   .option("url", jdbcUrl) \
// MAGIC   .option("forward_spark_azure_storage_credentials", "true") \
// MAGIC   .option("dbtable", "ADB_EVENTS_FLIGHTS_DW_TBL") \
// MAGIC   .option("tempdir", "wasbs://sqldw-temp@adbeventsdemo.blob.core.windows.net/temp") \
// MAGIC   .saveAsTable("ADB_EVENTS_FLIGHTS_DW_TBL")

// COMMAND ----------

// MAGIC %sql DESC TABLE ADB_EVENTS_FLIGHTS_DW_TBL

// COMMAND ----------

// MAGIC %sql SELECT * FROM ADB_EVENTS_FLIGHTS_DW_TBL