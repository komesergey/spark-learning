package com.spark.learning

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

/*
    3. Compute the highest-rated movie per year and include all the actors in that
    movie. The output should have only one movie per year, and it should contain
    four columns: year, movie title, rating, and a semicolon-separated list of
    actor names. This question will require joining the movies.tsv and movie-ratings.
    tsv files.

    (1066,((2012,12.8205),None))
    (Psycho,((1960,10.6375),None))
    (Monty Python and the Holy Grail,((1975,8.6584),None))
    (Cinderella,((1950,9.4226),None))
    (One Hundred and One Dalmatians,((1961,0.6726),Some(List(Wickes, Mary, Wright, Ben (I)))))
    (Sleeping Beauty,((1959,6.3919),None))
    (It's a Mad Mad Mad Mad World,((1963,6.626),None))
    (Leave It to Beaver,((1997,13.7106),None))
    (The Love Bug,((1968,13.4383),None))
    (Sgt. Bilko,((1996,14.1),None))
 */
object SparkRDDTaskThree {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder.appName("SparkRDDTaskOne")
      .config("spark.master", "local")
      .config("driver-memory", "10G")
      .config("executor-memory", "15G")
      .config("executor-cores", "8")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    val movieRatings = spark.sparkContext.textFile("movie-ratings.tsv")
      .map(_.split("\t"))
      .map(row => (row(2), (row(1), row(0).toDouble)))
      .groupByKey()
      .map(x => (x._1, x._2.toList.sortWith(_._2 >= _._2).head))
      .map(row => (row._2._1, (row._1, row._2._2)))

    val movies = spark.sparkContext.textFile("movies.tsv")
      .map(_.split("\t"))
      .map(row => (row(1), row(0)))
      .groupByKey()
      .map(row => (row._1, row._2.toList))

    val moviesWithRatings = movieRatings.leftOuterJoin(movies)

    moviesWithRatings.take(10).foreach(println)
  }
}
