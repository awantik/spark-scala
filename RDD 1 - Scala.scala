// Databricks notebook source
// MAGIC %fs ls /databricks-datasets/

// COMMAND ----------

val rdd = sc.textFile("/databricks-datasets/README.md")

// COMMAND ----------

rdd.collect()

// COMMAND ----------

val d = rdd.map(l => l.split(",")).collect()

// COMMAND ----------

val input = sc.parallelize(List(1,2,3,4))

// COMMAND ----------

input.flatMap(x => List(x,x+1,x+2)).collect()

// COMMAND ----------

input.map(x => List(x,x+1,x+2)).collect()

// COMMAND ----------

val parallel = sc.parallelize( 1 to 9, 3)

// COMMAND ----------

parallel.glom().collect()

// COMMAND ----------

parallel.mapPartitions( x => List(x.next).iterator).collect()

// COMMAND ----------

parallel.map( x => x).collect()

// COMMAND ----------

val l = List(1,2,3)

// COMMAND ----------

val vit = l.iterator

// COMMAND ----------

val d = List(vit.next).iterator

// COMMAND ----------

val parallel = sc.parallelize( 1 to 9)

// COMMAND ----------

parallel.mapPartitions( x => List(x.next).iterator).collect

// COMMAND ----------

parallel.mapPartitionsWithIndex(( index:Int, it:Iterator[Int]) => it.toList.map( x=> index + " " + x).iterator).collect

// COMMAND ----------

parallel.glom().collect()

// COMMAND ----------

val rdd1 = sc.parallelize(List(1,2,3,4))
val rdd2 = sc.parallelize(List(4,5,6,7))

// COMMAND ----------

rdd1.union(rdd2).distinct().collect()

// COMMAND ----------

rdd1.subtract(rdd2).collect()

// COMMAND ----------

rdd1.intersection(rdd2).collect()

// COMMAND ----------

rdd1.cartesian(rdd2).collect()

// COMMAND ----------

rdd1.filter( x => x%2 == 0).collect()

// COMMAND ----------

val rdd1 = sc.parallelize(List(1,2,4,1,1,3,9,6))

// COMMAND ----------

rdd1.groupBy(x => x ).map(x => x._2.sum).collect()

// COMMAND ----------

val l = List(1,2,3)

// COMMAND ----------

l.sum

// COMMAND ----------

rdd1.reduce( _ + _)

// COMMAND ----------

rdd1.reduce( (x:Int,y:Int) => x+y)

// COMMAND ----------

rdd1.countByValue()

// COMMAND ----------

rdd1.foreach(println)

// COMMAND ----------

rdd1.take(3)

// COMMAND ----------

rdd1.collect()

// COMMAND ----------

rdd1.top(4)

// COMMAND ----------

// MAGIC %md
// MAGIC <h3>Understanding lineage</h3>

// COMMAND ----------

val a = sc.parallelize( 1 to 5, 4)

// COMMAND ----------

a.flatMap( x => List(x,x,x)).collect

// COMMAND ----------

val z = sc.parallelize(List("red","blue","green","yellow"))

// COMMAND ----------

z.map( x => (x.length,x)).keys.collect

// COMMAND ----------

z.map( x => (x.length,x)).values.collect

// COMMAND ----------

// MAGIC %md
// MAGIC <h3>Transformations on pair RDDs</h3>

// COMMAND ----------

val a = sc.parallelize(List("black", "blue", "white", "green", "grey"), 2) 

// COMMAND ----------

val b = a.keyBy(_.length)

// COMMAND ----------

b.collect

// COMMAND ----------

a.keyBy(x => x.length).groupByKey.collect

// COMMAND ----------

val a = sc.parallelize(List("blue", "green", "orange"), 3) 
val b = a.keyBy(_.length)


// COMMAND ----------

val c = sc.parallelize(List("black", "white", "grey"), 3) 
val d = c.keyBy(_.length)

// COMMAND ----------

b.leftOuterJoin(d).collect

// COMMAND ----------

b.collect

// COMMAND ----------

d.collect

// COMMAND ----------

//rightOuterJoin
//fullOuterJoin

// COMMAND ----------

b.collect

// COMMAND ----------

b.reduceByKey(_ + _).collect

// COMMAND ----------

val c = sc.parallelize(List("black", "white", "grey","green")) 
val d = c.keyBy(_.length)

// COMMAND ----------

d.reduceByKey(_ + _).collect

// COMMAND ----------

val z = sc.parallelize(List(1,2,7,4,30,6), 3) 

// COMMAND ----------

z.glom.collect
//z.aggregate(0)()

// COMMAND ----------

// 1st arg func - takes 2 arg, returns 1, This gets executed for each partition
// 2nd arg fun - take 2 results iteratively from all partitions & retuns the result
// the zero value will be used for each operation
z.aggregate(0)(math.max( _ , _ ), _ + _ )

// COMMAND ----------


