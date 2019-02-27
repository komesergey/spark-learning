package com.spark.learning

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/*
    1. Compute the number of movies produced in each year. The output should have
    two columns: year and count. The output should be ordered by the count in
    descending order.

    (2006,2078)
    (2004,2005)
    (2007,1986)
    (2005,1960)
    (2011,1926)
    (2008,1892)
    (2009,1890)
    (2010,1843)
    (2002,1834)
    (2001,1687)
 */
object SparkRDDTaskOne {
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
      .map(row => (row(2), 1))
      .reduceByKey((total, value) => total + value)
      .map(row => (row._2, row._1))
      .sortByKey(ascending = false)
      .map(row => (row._2, row._1))
      .take(10)
      .foreach(x =>  println(x._1, x._2))
  }
}
