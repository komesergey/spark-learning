package com.spark.learning

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/*
    -------------------------------------------
    Batch: 0
    -------------------------------------------
    +------------------------------------------+-----+
    |window                                    |count|
    +------------------------------------------+-----+
    |[2019-02-27 12:40:00, 2019-02-27 12:50:00]|1    |
    +------------------------------------------+-----+

    -------------------------------------------
    Batch: 1
    -------------------------------------------
    +------------------------------------------+-----+
    |window                                    |count|
    +------------------------------------------+-----+
    |[2019-02-27 12:40:00, 2019-02-27 12:50:00]|1    |
    |[2019-02-27 12:50:00, 2019-02-27 13:00:00]|1    |
    +------------------------------------------+-----+

    -------------------------------------------
    Batch: 2
    -------------------------------------------
    +------------------------------------------+-----+
    |window                                    |count|
    +------------------------------------------+-----+
    |[2019-02-27 12:40:00, 2019-02-27 12:50:00]|1    |
    |[2019-02-27 12:50:00, 2019-02-27 13:00:00]|2    |
    +------------------------------------------+-----+
 */
object KafkaSparkStreamingOverFixedWindow {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder.appName("KafkaSparkStreaming")
      .config("spark.master", "local")
      .config("driver-memory", "10G")
      .config("executor-memory", "15G")
      .config("executor-cores", "8")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._
    val df = spark.readStream
      .format("kafka")
      .option("startingOffsets","earliest")
      .option("kafka.bootstrap.servers", "localhost:9091,localhost:9092,localhost:9093")
      .option("subscribe", "telemetry-replicated").load()
    val str = df.selectExpr("CAST(timestamp as TIMESTAMP)", "CAST(key AS STRING)", "CAST(value AS STRING)")
    val ssd = str.groupBy(window($"timestamp", "10 minutes")).count
      .writeStream.format("console")
      .option("truncate","false")
      .outputMode("complete")
      .start()
    //val str = newDf.writeStream.format("csv").option("checkpointLocation", "./tmp/streaming").outputMode("append").start("./kafka-sink")
    ssd.awaitTermination()
  }
}
