package com.spark.learning

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/*
    2. Compute the number of movies each actor was in. The output should have
    two columns: actor and count. The output should be ordered by the count in
    descending order.

    (Welker, Frank,38)
    (Tatasciore, Fred,38)
    (Jackson, Samuel L.,32)
    (Harnell, Jess,31)
    (Willis, Bruce,27)
    (Damon, Matt,27)
    (Cummings, Jim (I),26)
    (McGowan, Mickie,25)
    (Lynn, Sherry (I),25)
    (Bergen, Bob (I),25)
 */
object SparkRDDTaskTwo {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder.appName("SparkRDDTaskOne")
      .config("spark.master", "local")
      .config("driver-memory", "10G")
      .config("executor-memory", "15G")
      .config("executor-cores", "8")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark.sparkContext.textFile("movies.tsv")
      .map(_.split("\t"))
      .map(row => (row(0), row(1)))
      .map(row => (row._1, 1))
      .reduceByKey((total, value) => total + value)
      .map(row => (row._2, row._1))
      .sortByKey(ascending = false)
      .map(row => (row._2, row._1))
      .take(10)
      .foreach(x => println(x._1, x._2))
  }
}
