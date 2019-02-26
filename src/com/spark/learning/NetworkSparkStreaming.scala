package com.spark.learning

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

//nc -lk 9999
object NetworkSparkStreaming {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder.appName("StructuredNetworkWordCount")
      .config("spark.master", "local")
      .config("driver-memory", "10G")
      .config("executor-memory", "15G")
      .config("executor-cores", "8")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._
    val lines = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load()
    val words = lines.as[String].flatMap(_.split(" "))
    val wordCounts = words.groupBy("value").count()
    val query = wordCounts.writeStream
      .outputMode("complete")
      .format("console")
      .start()
    query.awaitTermination()
  }
}
