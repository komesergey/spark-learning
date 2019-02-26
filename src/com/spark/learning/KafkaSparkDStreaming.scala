package com.spark.learning

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming._

object KafkaSparkDStreaming {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder.appName("KafkaSparkStreaming")
      .config("spark.master", "local")
      .config("driver-memory", "10G")
      .config("executor-memory", "15G")
      .config("executor-cores", "8")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    val params = Map[String, Object](
      "bootstrap.servers" -> "localhost:9091,localhost:9092,localhost:9093",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "group",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean)
    )
    val topic = "telemetry-replicated".split(",").toSet
    val ssc = new StreamingContext(spark.sparkContext, Seconds(1))
    val stream = KafkaUtils.createDirectStream[String, String](ssc, PreferConsistent, Subscribe[String, String](topic, params))
    stream.foreachRDD { rdd =>
      rdd.foreach { record =>
        println(record.key, record.value)
      }
    }
    ssc.start()
    ssc.awaitTermination()
  }
}
