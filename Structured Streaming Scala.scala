// Databricks notebook source
// MAGIC %fs ls /databricks-datasets/structured-streaming/events/

// COMMAND ----------

// MAGIC %fs head /databricks-datasets/structured-streaming/events/file-0.json

// COMMAND ----------

// MAGIC %md
// MAGIC <h3>Batch/Interactive Processing</h3>

// COMMAND ----------

import org.apache.spark.sql.types._
val inputPath = "/databricks-datasets/structured-streaming/events/"

//Schema Creation
val jsonSchema = new StructType().add("time", TimestampType).add("action", StringType)

val staticInputDF = spark
    .read
    .schema(jsonSchema)
    .json(inputPath)

// COMMAND ----------

display(staticInputDF)

// COMMAND ----------

import org.apache.spark.sql.functions._

val staticCountsDF = 
  staticInputDF
    .groupBy($"action", window($"time", "1 minute"))
    .count() 

// COMMAND ----------

staticCountsDF.createOrReplaceTempView("static_counts")

// COMMAND ----------

// MAGIC %sql select action, sum(count) as total_count from static_counts group by action

// COMMAND ----------

// MAGIC %sql select action, date_format(window.end, "MMM-dd HH:mm") as time, count from static_counts order by time, action

// COMMAND ----------

// MAGIC %md
// MAGIC <h3>Stream Processing</h3>

// COMMAND ----------

import org.apache.spark.sql.functions._

//creating stream generator
val streamingInputDF = 
  spark
    .readStream                       // `readStream` instead of `read` for creating streaming DataFrame
    .schema(jsonSchema)               // Set the schema of the JSON data
    .option("maxFilesPerTrigger", 1)  // Treat a sequence of files as a stream by picking one file at a time
    .json(inputPath)

// COMMAND ----------

// Same query as staticInputDF
val streamingCountsDF = 
  streamingInputDF
    .groupBy($"action", window($"time", "1 hour"))
    .count()

// Is this DF actually a streaming DF?
streamingCountsDF.isStreaming

// COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", "1")

val query =
  streamingCountsDF
    .writeStream
    .format("memory")        // memory = store in-memory table (for testing only in Spark 2.0)
    .queryName("counts")     // counts = name of the in-memory table
    .outputMode("complete")  // complete = all the counts should be in the table
    .start()

// COMMAND ----------

// MAGIC %sql select action, date_format(window.end, "MMM-dd HH:mm") as time, count from counts order by time, action

// COMMAND ----------

// MAGIC %sql select action, date_format(window.end, "MMM-dd HH:mm") as time, count from counts order by time, action

// COMMAND ----------

mport org.apache.spark.sql.functions.{explode, split}

// Setup connection to Kafka
val kafka = spark.readStream
  .format("kafka")
  .option("kafka.bootstrap.servers", "YOUR.HOST:PORT1,YOUR.HOST:PORT2")   // comma separated list of broker:host
  .option("subscribe", "YOUR_TOPIC1,YOUR_TOPIC2")    // comma separated list of topics
  .option("startingOffsets", "latest") // read data from the end of the stream
  .load()

// split lines by whitespace and explode the array as rows of `word`
val df = kafka.select(explode(split($"value".cast("string"), "\\s+")).as("word"))
  .groupBy($"word")
  .count

