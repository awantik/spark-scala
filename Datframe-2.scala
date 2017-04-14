// Databricks notebook source
import org.apache.spark.sql.types._

val our_schema = new StructType(Array( 
  new StructField("age",LongType, false),
  new StructField("workclass", StringType, false),
  new StructField("fnlwgt", DoubleType, false)
))

// COMMAND ----------

val df = spark.read.format("csv")  .option("header", "false")  
.option("inferSchema", "true") 
.schema(our_schema)
.load("/databricks-datasets/adult/adult.data")

// COMMAND ----------

// MAGIC %fs ls /databricks-datasets/adult

// COMMAND ----------

display(df)

// COMMAND ----------

df.show()

// COMMAND ----------

import org.apache.spark.sql.functions._

// COMMAND ----------

df.col("age").show()

// COMMAND ----------

df.select(df.col("age") > 50)

// COMMAND ----------

df.select("age" > 50)

// COMMAND ----------

val newdf = df.select(col("age"), col("fnlwgt"),col("age")+col("fnlwgt"))

// COMMAND ----------

newdf.show()

// COMMAND ----------

df.filter(col("age") > 50 ).select("age").show()

// COMMAND ----------

val df1 = df.withColumn("numOne",lit(100))

// COMMAND ----------

val df2 = df1.drop("fnlwgt")

// COMMAND ----------

df2.show()

// COMMAND ----------

df.withColumn("age", col("age").cast(StringType)).printSchema()

// COMMAND ----------

df.filter(col("age") =!= 40).show()

// COMMAND ----------

df.filter(col("age") === 40).show()

// COMMAND ----------

df.select("age").distinct().count()

// COMMAND ----------

val df5 = df.randomSplit(Array(0.25, 0.75), 10)

// COMMAND ----------

df5(0).count()

// COMMAND ----------

df5(1).count()

// COMMAND ----------

df.
