package com.spark.learning

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

//./kafka-topics.sh --create --zookeeper localhost:2181,localhost:2182,localhost:2183 --replication-factor 3 --partitions 1 --topic telemetry-replicated

//./kafka-topics.sh --describe --zookeeper localhost:2181,localhost:2182,localhost:2183 --topic telemetry-replicated

//./kafka-console-producer.sh --broker-list localhost:9091,localhost:9092,localhost:9093 --topic telemetry-replicated

//./kafka-console-consumer.sh --bootstrap-server localhost:9091,localhost:9092,localhost:9093 --topic telemetry-replicated --property "parse.key=true" --property "key.separator=:" --from-beginning

object KafkaSparkStreaming {
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
    val newDf = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)").as[(String, String)]
    val str = newDf.writeStream.format("console").outputMode("append").start()
    //val str = newDf.writeStream.format("csv").option("checkpointLocation", "./tmp/streaming").outputMode("append").start("./kafka-sink")
    str.awaitTermination()
  }
}
