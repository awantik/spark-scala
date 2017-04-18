// Databricks notebook source
val person = Seq(
    (0, "Bill Chambers", 0, Seq(100)),
    (1, "Matei Zaharia", 1, Seq(500, 250, 100)),
    (2, "Michael Armbrust", 1, Seq(250, 100)))
  .toDF("id", "name", "graduate_program", "spark_status")

// COMMAND ----------

display(person)

// COMMAND ----------

val graduateProgram = Seq(
    (0, "Masters", "School of Information", "UC Berkeley"),
    (2, "Masters", "EECS", "UC Berkeley"),
    (1, "Ph.D.", "EECS", "UC Berkeley"))
  .toDF("id", "degree", "department", "school")

// COMMAND ----------

display(graduateProgram)

// COMMAND ----------

val sparkStatus = Seq(
    (500, "Vice President"),
    (250, "PMC Member"),
    (100, "Contributor"))
  .toDF("id", "status")

// COMMAND ----------

val joinExpression = person.col("graduate_program") === graduateProgram.col("id")

// COMMAND ----------

person.join(graduateProgram, joinExpression, "inner").show()

// COMMAND ----------

person.join(graduateProgram, joinExpression, "left_outer").drop(person.col("graduate_program")).show()

// COMMAND ----------

val data = person.join(graduateProgram, joinExpression, "left_outer").drop(person.col("graduate_program"))

// COMMAND ----------

data.drop("id").show()

// COMMAND ----------

case class Person(name: String, age: Long)

// Encoders are created for case classes
val caseClassDS = Seq(Person("Andy", 32)).toDS()
caseClassDS.show()

// COMMAND ----------

val primitiveDS = Seq(1, 2, 3).toDS()
primitiveDS.map(_ + 1).collect()

// COMMAND ----------

primitiveDS.first

// COMMAND ----------


