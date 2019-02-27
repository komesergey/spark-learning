package com.spark.learning

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

/*
    1. Compute the number of movies produced in each year.
    The output should have two columns: actor and count.
    The output should be ordered by the count in descending order.


    +----+-----+
    |year|count|
    +----+-----+
    |2006| 2078|
    |2004| 2005|
    |2007| 1986|
    |2005| 1960|
    |2011| 1926|
    |2008| 1892|
    |2009| 1890|
    |2010| 1843|
    |2002| 1834|
    |2001| 1687|
    +----+-----+
    only showing top 10 rows


    == Parsed Logical Plan ==
    GlobalLimit 6
    +- LocalLimit 6
       +- Project [cast(year#6263 as string) AS year#6274, cast(count#6267L as string) AS count#6275]
          +- Sort [count#6267L DESC NULLS LAST], true
             +- Aggregate [year#6263], [year#6263, count(1) AS count#6267L]
                +- SubqueryAlias `movies`
                   +- LogicalRDD [actor#6261, movie#6262, year#6263], false

    == Analyzed Logical Plan ==
    year: string, count: string
    GlobalLimit 6
    +- LocalLimit 6
       +- Project [cast(year#6263 as string) AS year#6274, cast(count#6267L as string) AS count#6275]
          +- Sort [count#6267L DESC NULLS LAST], true
             +- Aggregate [year#6263], [year#6263, count(1) AS count#6267L]
                +- SubqueryAlias `movies`
                   +- LogicalRDD [actor#6261, movie#6262, year#6263], false

    == Optimized Logical Plan ==
    GlobalLimit 6
    +- LocalLimit 6
       +- Project [cast(year#6263 as string) AS year#6274, cast(count#6267L as string) AS count#6275]
          +- Sort [count#6267L DESC NULLS LAST], true
             +- Aggregate [year#6263], [year#6263, count(1) AS count#6267L]
                +- Project [year#6263]
                   +- LogicalRDD [actor#6261, movie#6262, year#6263], false

    == Physical Plan ==
    TakeOrderedAndProject(limit=6, orderBy=[count#6267L DESC NULLS LAST], output=[year#6274,count#6275])
    +- *(2) HashAggregate(keys=[year#6263], functions=[count(1)], output=[year#6263, count#6267L])
       +- Exchange hashpartitioning(year#6263, 200)
          +- *(1) HashAggregate(keys=[year#6263], functions=[partial_count(1)], output=[year#6263, count#6279L])
             +- *(1) Project [year#6263]
                +- Scan ExistingRDD[actor#6261,movie#6262,year#6263]
 */
object SparkSQLTaskOne {
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

    spark.sql("select year, count(*) as count " +
      "from movies " +
      "group by year " +
      "order by count desc").show(10)
  }
}
