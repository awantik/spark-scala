// Databricks notebook source
val studentmarks = Array(("Ankit",55), ("Arijit",88), ("Ankit",35), ("Arijit",98), ("Ankit",95))

// COMMAND ----------

val rdd = sc.parallelize(studentmarks,2)

// COMMAND ----------

type marksAcc = (Int, Double)

// COMMAND ----------

type StudentMarks = (String, (Int, Double))

// COMMAND ----------

val studentmarks = Array(("Ankit",55.0), ("Arijit",88.0), ("Ankit",35.0), ("Arijit",98.0), ("Ankit",95.0))

// COMMAND ----------

val rdd = sc.parallelize(studentmarks,2).cache()

// COMMAND ----------

// combinerFunc will be created per partition per key
val combinerFunc = (marks: Double) => (1, marks)

// COMMAND ----------

// This executes per partition input to these are cominer data with new values for same key
val seqCombiner = ( acc: marksAcc, marks: Double) => (acc._1 +1, acc._2+marks)

// COMMAND ----------

val partCombiner = ( acc1: marksAcc, acc2: marksAcc) => (acc1._1 + acc2._1, acc1._2 + acc2._2)

// COMMAND ----------

rdd.combineByKey(combinerFunc, seqCombiner, partCombiner).map(x => (x._1,x._2._2/x._2._1 )).collect()

// COMMAND ----------

rdd.combineByKey(combinerFunc, seqCombiner, partCombiner).map(x => (x._1,x._2)).collect()

// COMMAND ----------

val x = sc.parallelize(1 to 10)

// COMMAND ----------

val y = sc.parallelize(11 to 20)

// COMMAND ----------

x

// COMMAND ----------

y

// COMMAND ----------

x.zip(y).collect()

// COMMAND ----------


