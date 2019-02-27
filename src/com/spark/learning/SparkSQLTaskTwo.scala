package com.spark.learning

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

/*
    2. Compute the number of movies each actor was in. The output should have
    two columns: actor and count. The output should be ordered by the count in
    descending order.


    +------------------+-----+
    |             actor|count|
    +------------------+-----+
    |     Welker, Frank|   38|
    |  Tatasciore, Fred|   38|
    |Jackson, Samuel L.|   32|
    |     Harnell, Jess|   31|
    |     Willis, Bruce|   27|
    |       Damon, Matt|   27|
    | Cummings, Jim (I)|   26|
    |  Lynn, Sherry (I)|   25|
    |   McGowan, Mickie|   25|
    |        Hanks, Tom|   25|
    +------------------+-----+
    only showing top 10 rows


    == Parsed Logical Plan ==
    GlobalLimit 6
    +- LocalLimit 6
       +- Project [cast(actor#6261 as string) AS actor#6288, cast(count#6281L as string) AS count#6289]
          +- Sort [count#6281L DESC NULLS LAST], true
             +- Aggregate [actor#6261], [actor#6261, count(1) AS count#6281L]
                +- SubqueryAlias `movies`
                   +- LogicalRDD [actor#6261, movie#6262, year#6263], false

    == Analyzed Logical Plan ==
    actor: string, count: string
    GlobalLimit 6
    +- LocalLimit 6
       +- Project [cast(actor#6261 as string) AS actor#6288, cast(count#6281L as string) AS count#6289]
          +- Sort [count#6281L DESC NULLS LAST], true
             +- Aggregate [actor#6261], [actor#6261, count(1) AS count#6281L]
                +- SubqueryAlias `movies`
                   +- LogicalRDD [actor#6261, movie#6262, year#6263], false

    == Optimized Logical Plan ==
    GlobalLimit 6
    +- LocalLimit 6
       +- Project [actor#6261, cast(count#6281L as string) AS count#6289]
          +- Sort [count#6281L DESC NULLS LAST], true
             +- Aggregate [actor#6261], [actor#6261, count(1) AS count#6281L]
                +- Project [actor#6261]
                   +- LogicalRDD [actor#6261, movie#6262, year#6263], false

    == Physical Plan ==
    TakeOrderedAndProject(limit=6, orderBy=[count#6281L DESC NULLS LAST], output=[actor#6261,count#6289])
    +- *(2) HashAggregate(keys=[actor#6261], functions=[count(1)], output=[actor#6261, count#6281L])
       +- Exchange hashpartitioning(actor#6261, 200)
          +- *(1) HashAggregate(keys=[actor#6261], functions=[partial_count(1)], output=[actor#6261, count#6293L])
             +- *(1) Project [actor#6261]
                +- Scan ExistingRDD[actor#6261,movie#6262,year#6263]
 */
object SparkSQLTaskTwo {
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

    val schema = StructType(Array(
      StructField("actor", StringType,  nullable = false),
      StructField("movie", StringType,  nullable = false),
      StructField("year",  IntegerType, nullable = false)
    ))

    val moviesDF = spark.createDataFrame(moviesRdd, schema)

    moviesDF.createOrReplaceTempView("movies")

    spark.sql("select actor, count(*) as count " +
      "from movies " +
      "group by actor " +
      "order by count desc").show(10)
  }
}
