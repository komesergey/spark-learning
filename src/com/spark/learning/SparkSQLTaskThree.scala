package com.spark.learning

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._

/*
    3. Compute the highest-rated movie per year and include all the actors in that
    movie. The output should have only one movie per year, and it should contain
    four columns: year, movie title, rating, and a semicolon-separated list of
    actor names.


    +--------------------+---------+----+--------------------+
    |               movie|maxRating|year| collect_list(actor)|
    +--------------------+---------+----+--------------------+
    |           Beginners|  14.2173|2010|[Plummer, Christo...|
    |    An American Tail|  14.2122|1986|[Finnegan, John (...|
    |           Sleepover|  14.2073|2004|                  []|
    |             The Man|  14.1976|2005|[Jackson, Samuel ...|
    |               Gigli|  14.1829|2003|[Lopez, Jennifer ...|
    |Stop! Or My Mom W...|  14.1621|1992|                  []|
    |Ang babae sa sept...|  14.1527|2011|                  []|
    |The Inbetweeners ...|  14.1507|2011|                  []|
    |Änglagård - Andra...|  14.1242|1994|                  []|
    |              Taxi 2|  14.1178|2000|                  []|
    +--------------------+---------+----+--------------------+
    only showing top 10 rows


    == Parsed Logical Plan ==
    GlobalLimit 21
    +- LocalLimit 21
       +- Project [cast(movie#6299 as string) AS movie#6317, cast(maxRating#6304 as string) AS maxRating#6318, cast(year#6300 as string) AS year#6319, cast(collect_list(actor)#6307 as string) AS collect_list(actor)#6320]
          +- Sort [maxRating#6304 DESC NULLS LAST], true
             +- Aggregate [year#6300, movie#6299], [movie#6299, max(rating#6298) AS maxRating#6304, year#6300, collect_list(actor#6261, 0, 0) AS collect_list(actor)#6307]
                +- Join LeftOuter, (movie#6262 = movie#6299)
                   :- SubqueryAlias `movieratings`
                   :  +- LogicalRDD [rating#6298, movie#6299, year#6300], false
                   +- SubqueryAlias `movies`
                      +- LogicalRDD [actor#6261, movie#6262, year#6263], false

    == Analyzed Logical Plan ==
    movie: string, maxRating: string, year: string, collect_list(actor): string
    GlobalLimit 21
    +- LocalLimit 21
       +- Project [cast(movie#6299 as string) AS movie#6317, cast(maxRating#6304 as string) AS maxRating#6318, cast(year#6300 as string) AS year#6319, cast(collect_list(actor)#6307 as string) AS collect_list(actor)#6320]
          +- Sort [maxRating#6304 DESC NULLS LAST], true
             +- Aggregate [year#6300, movie#6299], [movie#6299, max(rating#6298) AS maxRating#6304, year#6300, collect_list(actor#6261, 0, 0) AS collect_list(actor)#6307]
                +- Join LeftOuter, (movie#6262 = movie#6299)
                   :- SubqueryAlias `movieratings`
                   :  +- LogicalRDD [rating#6298, movie#6299, year#6300], false
                   +- SubqueryAlias `movies`
                      +- LogicalRDD [actor#6261, movie#6262, year#6263], false

    == Optimized Logical Plan ==
    GlobalLimit 21
    +- LocalLimit 21
       +- Project [movie#6299, cast(maxRating#6304 as string) AS maxRating#6318, cast(year#6300 as string) AS year#6319, cast(collect_list(actor)#6307 as string) AS collect_list(actor)#6320]
          +- Sort [maxRating#6304 DESC NULLS LAST], true
             +- Aggregate [year#6300, movie#6299], [movie#6299, max(rating#6298) AS maxRating#6304, year#6300, collect_list(actor#6261, 0, 0) AS collect_list(actor)#6307]
                +- Project [rating#6298, movie#6299, year#6300, actor#6261]
                   +- Join LeftOuter, (movie#6262 = movie#6299)
                      :- LogicalRDD [rating#6298, movie#6299, year#6300], false
                      +- Project [actor#6261, movie#6262]
                         +- LogicalRDD [actor#6261, movie#6262, year#6263], false

    == Physical Plan ==
    TakeOrderedAndProject(limit=21, orderBy=[maxRating#6304 DESC NULLS LAST], output=[movie#6299,maxRating#6318,year#6319,collect_list(actor)#6320])
    +- ObjectHashAggregate(keys=[year#6300, movie#6299], functions=[max(rating#6298), collect_list(actor#6261, 0, 0)], output=[movie#6299, maxRating#6304, year#6300, collect_list(actor)#6307])
       +- ObjectHashAggregate(keys=[year#6300, movie#6299], functions=[partial_max(rating#6298), partial_collect_list(actor#6261, 0, 0)], output=[year#6300, movie#6299, max#6327, buf#6328])
          +- *(4) Project [rating#6298, movie#6299, year#6300, actor#6261]
             +- SortMergeJoin [movie#6299], [movie#6262], LeftOuter
                :- *(1) Sort [movie#6299 ASC NULLS FIRST], false, 0
                :  +- Exchange hashpartitioning(movie#6299, 200)
                :     +- Scan ExistingRDD[rating#6298,movie#6299,year#6300]
                +- *(3) Sort [movie#6262 ASC NULLS FIRST], false, 0
                   +- Exchange hashpartitioning(movie#6262, 200)
                      +- *(2) Project [actor#6261, movie#6262]
                         +- Scan ExistingRDD[actor#6261,movie#6262,year#6263]

 */
object SparkSQLTaskThree {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder.appName("SparkRDDTaskOne")
      .config("spark.master", "local")
      .config("driver-memory", "10G")
      .config("executor-memory", "15G")
      .config("executor-cores", "8")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    val moviesRdd = spark.sparkContext.textFile("movies.tsv")
      .map(_.split("\t"))
      .map(x=> Row(x(0), x(1), x(2).toInt))

    var schema = StructType(Array(
      StructField("actor", StringType,  nullable = false),
      StructField("movie", StringType,  nullable = false),
      StructField("year",  IntegerType, nullable = false)
    ))

    val moviesDF = spark.createDataFrame(moviesRdd, schema)

    moviesDF.createOrReplaceTempView("movies")

    val movieRatings = spark.sparkContext.textFile("movie-ratings.tsv")
      .map(_.split("\t"))
      .map(row => Row(row(0).toDouble, row(1), row(2).toInt))

    schema = StructType(Array(
      StructField("rating", DoubleType,  nullable = false),
      StructField("movie",  StringType,  nullable = false),
      StructField("year",   IntegerType, nullable = false)
    ))

    val moviesRatingsDF = spark.createDataFrame(movieRatings, schema)

    moviesRatingsDF.createOrReplaceTempView("movieRatings")

    spark.sql("select movieRatings.movie, max(rating) as maxRating, movieRatings.year, collect_list(movies.actor) " +
      "from movieRatings left outer join movies " +
      "on movies.movie == movieRatings.movie " +
      "group by movieRatings.year, movieRatings.movie " +
      "order by maxRating desc").show(10)
  }
}
