package com.spark.learning

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import scala.collection.mutable.ListBuffer

/*
    4. Determine which pair of actors worked together most. Working together
    is defined as appearing in the same movie. The output should have three
    columns: actor 1, actor 2, and count. The output should be sorted by the count
    in descending order. The solution to this question will require a self-join.


    (23,McGowan, Mickie,Lynn, Sherry (I))
    (19,Bergen, Bob (I),Lynn, Sherry (I))
    (19,Bergen, Bob (I),McGowan, Mickie)
    (17,Angel, Jack (I),Lynn, Sherry (I))
    (17,Angel, Jack (I),McGowan, Mickie)
    (16,Rabson, Jan,Lynn, Sherry (I))
    (16,Rabson, Jan,McGowan, Mickie)
    (15,Darling, Jennifer,McGowan, Mickie)
    (14,Darling, Jennifer,Lynn, Sherry (I))
    (14,Harnell, Jess,McGowan, Mickie)
    (14,Sandler, Adam (I),Schneider, Rob (I))
    (14,Bergen, Bob (I),Harnell, Jess)
    (14,Bergen, Bob (I),Rabson, Jan)
    (14,McGowan, Mickie,Farmer, Bill (I))
    (13,Harnell, Jess,Lynn, Sherry (I))
    (13,Bergen, Bob (I),Bumpass, Rodger)
    (13,Angel, Jack (I),Bergen, Bob (I))
    (13,Lynn, Sherry (I),Farmer, Bill (I))
    (12,Bergen, Bob (I),Farmer, Bill (I))
    (12,Bergen, Bob (I),Darling, Jennifer)
    (12,Proctor, Phil,Lynn, Sherry (I))
    (12,Covert, Allen,Sandler, Adam (I))
    (12,Angel, Jack (I),Darling, Jennifer)
    (12,Harnell, Jess,Rabson, Jan)
    (12,McGowan, Mickie,Bumpass, Rodger)
    (12,Derryberry, Debi,Lynn, Sherry (I))
    (12,Lynn, Sherry (I),Bumpass, Rodger)
    (12,Newman, Laraine,Mann, Danny (I))
    (12,Proctor, Phil,McGowan, Mickie)
    (11,Newman, Laraine,Lynn, Sherry (I))
    (11,Mann, Danny (I),Bergen, Bob (I))
    (11,Mann, Danny (I),McGowan, Mickie)
    (11,Cygan, John (I),McGowan, Mickie)
    (11,Tatasciore, Fred,Blum, Steve (IX))
    (11,Angel, Jack (I),Proctor, Phil)
    (11,Welker, Frank,Cummings, Jim (I))
    (11,Angel, Jack (I),Farmer, Bill (I))
    (11,Angel, Jack (I),Bumpass, Rodger)
    (11,North, Nolan,Tatasciore, Fred)
    (11,Mann, Danny (I),Lynn, Sherry (I))
    (11,Newman, Laraine,Rabson, Jan)
    (11,Derryberry, Debi,McGowan, Mickie)
    (11,Newman, Laraine,Bergen, Bob (I))
    (11,Mann, Danny (I),Harnell, Jess)
    (11,Cygan, John (I),Lynn, Sherry (I))
    (11,Newman, Laraine,McGowan, Mickie)
    (11,Newman, Laraine,Harnell, Jess)
    (11,Darling, Jennifer,Bumpass, Rodger)
    (11,Derryberry, Debi,Farmer, Bill (I))
    (11,Angel, Jack (I),Rabson, Jan)
 */
object SparkRDDTaskFour {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder.appName("SparkRDDTaskOne")
      .config("spark.master", "local")
      .config("driver-memory", "10G")
      .config("executor-memory", "15G")
      .config("executor-cores", "8")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    case class IgnoreOrderTuple[T]( a:T, b:T ) {
      override def equals( that:Any ) = that match {
        case that:IgnoreOrderTuple[T] => ( that canEqual this ) && (
          this.a == that.a && this.b == that.b ||
            this.a == that.b && this.b == that.a
          )
        case _ => false
      }
      override def canEqual( that:Any ) = that.isInstanceOf[IgnoreOrderTuple[T]]
      override def hashCode = a.hashCode + b.hashCode
    }

    val moviesRdd = spark.sparkContext.textFile("movies.tsv")
      .map(_.split("\t"))
      .map(row => (row(1), row(0)))

    val selfMovies = moviesRdd.join(moviesRdd)

    selfMovies.filter(row => row._2._1 != row._2._2)
      .map(row => (IgnoreOrderTuple(row._2._1, row._2._2), ListBuffer(row._1)))
      .reduceByKey((total,value) => total ++= value )
      .map(x => (x._1, x._2.distinct))
      .map(x => (x._1, x._2.size))
      .map(row => (row._2, row._1))
      .sortByKey(ascending = false)
      .map(row => (row._2, row._1))
      .take(50).foreach(x => println(x._2, x._1.a, x._1.b))
  }
}
