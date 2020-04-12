// Databricks notebook source
val studentData = sc.textFile("/FileStore/tables/StudentsPerformance.txt")
studentData.count()

// COMMAND ----------

studentData.take(3)

// COMMAND ----------

studentData.collect.foreach(println)

// COMMAND ----------

val linesWithGroupA = studentData.filter(line => line.contains("group A"))
linesWithGroupA.count()

// COMMAND ----------

val linesWithGroupB = studentData.filter(line => line.contains("group B"))
linesWithGroupB.count()

// COMMAND ----------

val counts = studentData.flatMap(line => line.split(" "))
                    .map(word => (word, 1))
                    .reduceByKey(_+_)

// COMMAND ----------

val allwords = studentData.flatMap(_.split("\\W+"))
allwords.collect.foreach(println)

// COMMAND ----------

val words = allwords.filter(!_.isEmpty)
words.collect.foreach(println)

// COMMAND ----------

val pairs = words.map((_,1))
pairs.collect.foreach(println)

// COMMAND ----------

val reducedByKey = pairs.reduceByKey(_+_)
reducedByKey.collect.foreach(println)

// COMMAND ----------

val allwords = studentData.flatMap(_.split("\\W+"))
val words = allwords.filter(!_.isEmpty)
val pairs = words.map((_,1))
val reducedByKey = pairs.reduceByKey(_+_)
val top5words = reducedByKey.takeOrdered(5)(Ordering[Int].reverse.on(_._2))
top5words.foreach(println) 

// COMMAND ----------

val df = studentData.toDF
df.show()

// COMMAND ----------

val df = studentData.toDF("Student Data")
df.show()

// COMMAND ----------


