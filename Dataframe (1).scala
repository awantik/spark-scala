// Databricks notebook source
// MAGIC %md 
// MAGIC <h3>Understanding DataFrames</h3>
// MAGIC <h4>Creation</h4>
// MAGIC * Create from rdd
// MAGIC * Create from files
// MAGIC * Create from collection

// COMMAND ----------

val colors = List("white","green","yellow","red","brown","pink")
val rdd = sc.parallelize(colors).map(x => (x,x.length))

// COMMAND ----------

val df = rdd.toDF("color","length")

// COMMAND ----------

display(df)

// COMMAND ----------

df.columns

// COMMAND ----------

val l = List(4,6,8,9,7)
val r1 = sc.parallelize(l)
val r2 = sc.parallelize(l.tail :+ 0)

// COMMAND ----------

r1.zip(r2).map( x => x._1 + x._2).collect()

// COMMAND ----------

val ll = List(1,2,3)

// COMMAND ----------

ll.tail :+ 8

// COMMAND ----------

val babynames = spark.read.option("inferSchema","true").option("header","true").csv("/FileStore/tables/1oiejwr41491830958743/baby_names_reduced.csv")

// COMMAND ----------

display(babynames)

// COMMAND ----------

display(babynames.select("Sex","Count"))





// COMMAND ----------

babynames.select("Sex","Count").show()

// COMMAND ----------

babynames.count()

// COMMAND ----------

babynames.printSchema()

// COMMAND ----------

babynames.describe().collect()

// COMMAND ----------

babynames.filter(babynames("Count") > 200).select("First Name").show()

// COMMAND ----------

babynames.groupBy("Sex").count().show()

// COMMAND ----------

// MAGIC %md
// MAGIC <h2>Create Dataframe</h2>

// COMMAND ----------

val rdd = sc.parallelize(List((1,2), (3,4)))

// COMMAND ----------

rdd.map( x => (x._1 +1, x._2 +1)).map(x => x._1).collect

// COMMAND ----------

// Create the case classes for our domain
case class Department(id: String, name: String)
case class Employee(firstName: String, lastName: String, email: String, salary: Int)
case class DepartmentWithEmployees(department: Department, employees: Seq[Employee])

// COMMAND ----------

// Create the Departments
val department1 = new Department("123456", "Computer Science")
val department2 = new Department("789012", "Mechanical Engineering")
val department3 = new Department("345678", "Theater and Drama")
val department4 = new Department("901234", "Indoor Recreation")

// COMMAND ----------

// Create the Employees
val employee1 = new Employee("michael", "armbrust", "no-reply@berkeley.edu", 100000)
val employee2 = new Employee("xiangrui", "meng", "no-reply@stanford.edu", 120000)
val employee3 = new Employee("matei", null, "no-reply@waterloo.edu", 140000)
val employee4 = new Employee(null, "wendell", "no-reply@princeton.edu", 160000)


// COMMAND ----------

// Create the DepartmentWithEmployees instances from Departments and Employees
val departmentWithEmployees1 = new DepartmentWithEmployees(department1, Seq(employee1, employee2))
val departmentWithEmployees2 = new DepartmentWithEmployees(department2, Seq(employee3, employee4))
val departmentWithEmployees3 = new DepartmentWithEmployees(department3, Seq(employee1, employee4))
val departmentWithEmployees4 = new DepartmentWithEmployees(department4, Seq(employee2, employee3))

// COMMAND ----------

// MAGIC %md
// MAGIC <h5>1st DF from Collection</h5>

// COMMAND ----------

val departmentsWithEmployeesSeq1 = Seq(departmentWithEmployees1, departmentWithEmployees2)
val df1 = departmentsWithEmployeesSeq1.toDF()

// COMMAND ----------

// MAGIC %md
// MAGIC <h5>2nd DF from Collection</h5>

// COMMAND ----------

val departmentsWithEmployeesSeq2 = Seq(departmentWithEmployees3, departmentWithEmployees4)
val df2 = departmentsWithEmployeesSeq2.toDF()

// COMMAND ----------



// COMMAND ----------

val uniondf = df1.union(df2)

// COMMAND ----------

display(uniondf)

// COMMAND ----------

// MAGIC %md
// MAGIC <h5>Write Unioned data in parquet </h5>

// COMMAND ----------

uniondf.write.parquet("/tmp/myfile1.parquet")

// COMMAND ----------

val rdd = sc.parallelize(List(("Awi",5),("Bwi",8)))

// COMMAND ----------

val parquetDF = spark.read.parquet("/tmp/myfile1.parquet")

// COMMAND ----------

display(parquetDF)

// COMMAND ----------

val eplodeDF = parquetDF.explode($"employees"){
  case Row(employee: Seq[Row]) => employee.map{ employee => 
    val firstName = employee(0).asInstanceOf[String]
    val lastname = employee(1).asInstanceOf[String]
    val email = employee(2).asInstanceOf[String]
    val salary = employee(3).asInstanceOf[Int]
    Employee(firstName, lastname, email, salary)
  
  }
}

// COMMAND ----------

display(eplodeDF)

// COMMAND ----------

val filterDF = eplodeDF.filter($"firstName" === "xiangrui" || $"firstName" === "michael").sort($"lastname".asc)

// COMMAND ----------

display(filterDF)

// COMMAND ----------

// MAGIC %md
// MAGIC <p>Replace null by -</p> 

// COMMAND ----------

val naFunctions = eplodeDF.na
val nonNullDF = naFunctions.fill("--")
display(nonNullDF)

// COMMAND ----------

val filterNonNullDF = nonNullDF.filter(!($"firstName" === "--" || $"firstName" === "--")).sort($"email".asc)

// COMMAND ----------

display(filterNonNullDF)

// COMMAND ----------

import org.apache.spark.sql.functions._

// COMMAND ----------


val countDistinctDF = eplodeDF.select($"firstName").groupBy($"firstName").agg(count($"firstName"))
display(countDistinctDF)

// COMMAND ----------

val countDistinctDF = eplodeDF.select($"firstName").groupBy().agg(count($"firstName"))
display(countDistinctDF)

// COMMAND ----------

val d_1 = nonNullDF.registerTempTable("zeke_df")
spark.sql("""
 SELECT * 
 FROM zeke_df
""")

// COMMAND ----------

display(nonNullDF)

// COMMAND ----------

nonNullDF.createOrReplaceTempView("zeke5")

// COMMAND ----------

val d_res = spark.sql("""
SELECT firstName, lastName as
FROM zeke5
""")

// COMMAND ----------

display(d_res)

// COMMAND ----------

nonNullDF.describe("salary").show()

// COMMAND ----------

// MAGIC %md
// MAGIC <h2>Flattening</h2>

// COMMAND ----------

()

// COMMAND ----------

val veryNestedDF = Seq(("1", (2, (3, 4)))).toDF()

// COMMAND ----------

display(veryNestedDF)

// COMMAND ----------

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

// COMMAND ----------

implicit class DataFrameFlattener( df: DataFrame){
  def flattenSchema: DataFrame = {
    df.select(flatten(Nil, df.schema) : _*)
  }
  
  def flatten(path: Seq[String], schema: DataType) : Seq[Column]= schema match {
    case s: StructType => s.fields.flatMap( f=> flatten(path :+ f.name, f.dataType ))
    case other => col(path.map(n => s"`$n`").mkString(".")).as(path.mkString(".")) :: Nil
 }
  
}

// COMMAND ----------

display(veryNestedDF.flattenSchema)

// COMMAND ----------

val dfnew = DataFrameFlattener(veryNestedDF)

// COMMAND ----------


