package com.spark.learning

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}


/*
    4. Determine which pair of actors worked together most. Working together
    is defined as appearing in the same movie. The output should have three
    columns: actor 1, actor 2, and count. The output should be sorted by the count
    in descending order. The solution to this question will require a self-join.


    +-----+-----------------+------------------+
    |count|            actor|             actor|
    +-----+-----------------+------------------+
    |   23| Lynn, Sherry (I)|   McGowan, Mickie|
    |   19|  Bergen, Bob (I)|  Lynn, Sherry (I)|
    |   19|  Bergen, Bob (I)|   McGowan, Mickie|
    |   17|  Angel, Jack (I)|  Lynn, Sherry (I)|
    |   17|  Angel, Jack (I)|   McGowan, Mickie|
    |   16|  McGowan, Mickie|       Rabson, Jan|
    |   16| Lynn, Sherry (I)|       Rabson, Jan|
    |   15|Darling, Jennifer|   McGowan, Mickie|
    |   14|  Bergen, Bob (I)|     Harnell, Jess|
    |   14|Darling, Jennifer|  Lynn, Sherry (I)|
    |   14|Sandler, Adam (I)|Schneider, Rob (I)|
    |   14| Farmer, Bill (I)|   McGowan, Mickie|
    |   14|    Harnell, Jess|   McGowan, Mickie|
    |   14|  Bergen, Bob (I)|       Rabson, Jan|
    |   13|    Harnell, Jess|  Lynn, Sherry (I)|
    |   13|  Angel, Jack (I)|   Bergen, Bob (I)|
    |   13|  Bergen, Bob (I)|   Bumpass, Rodger|
    |   13| Farmer, Bill (I)|  Lynn, Sherry (I)|
    |   12|  Bergen, Bob (I)| Darling, Jennifer|
    |   12|    Harnell, Jess|       Rabson, Jan|
    |   12| Derryberry, Debi|  Lynn, Sherry (I)|
    |   12|  Bumpass, Rodger|   McGowan, Mickie|
    |   12|  Bumpass, Rodger|  Lynn, Sherry (I)|
    |   12|  McGowan, Mickie|     Proctor, Phil|
    |   12|  Angel, Jack (I)| Darling, Jennifer|
    |   12|    Covert, Allen| Sandler, Adam (I)|
    |   12|  Mann, Danny (I)|   Newman, Laraine|
    |   12| Lynn, Sherry (I)|     Proctor, Phil|
    |   12|  Bergen, Bob (I)|  Farmer, Bill (I)|
    |   11|    Harnell, Jess|   Mann, Danny (I)|
    |   11|  Angel, Jack (I)|  Farmer, Bill (I)|
    |   11|  Cygan, John (I)|   McGowan, Mickie|
    |   11|  Angel, Jack (I)|       Rabson, Jan|
    |   11| Lynn, Sherry (I)|   Newman, Laraine|
    |   11|  Bumpass, Rodger| Darling, Jennifer|
    |   11|  Angel, Jack (I)|   Bumpass, Rodger|
    |   11| Lynn, Sherry (I)|   Mann, Danny (I)|
    |   11| Blum, Steve (IX)|  Tatasciore, Fred|
    |   11| Derryberry, Debi|  Farmer, Bill (I)|
    |   11|    Harnell, Jess|   Newman, Laraine|
    |   11|  Cygan, John (I)|  Lynn, Sherry (I)|
    |   11|  Mann, Danny (I)|   McGowan, Mickie|
    |   11|  Angel, Jack (I)|     Proctor, Phil|
    |   11|  Newman, Laraine|       Rabson, Jan|
    |   11| Farmer, Bill (I)|       Rabson, Jan|
    |   11|Cummings, Jim (I)|     Welker, Frank|
    |   11|     North, Nolan|  Tatasciore, Fred|
    |   11| Derryberry, Debi|   McGowan, Mickie|
    |   11|  McGowan, Mickie|   Newman, Laraine|
    |   11|  Bergen, Bob (I)|   Newman, Laraine|
    +-----+-----------------+------------------+
    only showing top 50 rows


    == Parsed Logical Plan ==
    GlobalLimit 51
    +- LocalLimit 51
       +- Project [cast(count#7730L as string) AS count#7742, cast(actor#6261 as string) AS actor#7743, cast(actor#7731 as string) AS actor#7744]
          +- Sort [count#7730L DESC NULLS LAST], true
             +- Aggregate [actor#7731, actor#6261], [count(movie#6262) AS count#7730L, actor#6261, actor#7731]
                +- Join Inner, ((movie#6262 = movie#7732) && (actor#6261 < actor#7731))
                   :- SubqueryAlias `m1`
                   :  +- SubqueryAlias `movies`
                   :     +- LogicalRDD [actor#6261, movie#6262, year#6263], false
                   +- SubqueryAlias `m2`
                      +- SubqueryAlias `movies`
                         +- LogicalRDD [actor#7731, movie#7732, year#7733], false

    == Analyzed Logical Plan ==
    count: string, actor: string, actor: string
    GlobalLimit 51
    +- LocalLimit 51
       +- Project [cast(count#7730L as string) AS count#7742, cast(actor#6261 as string) AS actor#7743, cast(actor#7731 as string) AS actor#7744]
          +- Sort [count#7730L DESC NULLS LAST], true
             +- Aggregate [actor#7731, actor#6261], [count(movie#6262) AS count#7730L, actor#6261, actor#7731]
                +- Join Inner, ((movie#6262 = movie#7732) && (actor#6261 < actor#7731))
                   :- SubqueryAlias `m1`
                   :  +- SubqueryAlias `movies`
                   :     +- LogicalRDD [actor#6261, movie#6262, year#6263], false
                   +- SubqueryAlias `m2`
                      +- SubqueryAlias `movies`
                         +- LogicalRDD [actor#7731, movie#7732, year#7733], false

    == Optimized Logical Plan ==
    GlobalLimit 51
    +- LocalLimit 51
       +- Project [cast(count#7730L as string) AS count#7742, actor#6261, actor#7731]
          +- Sort [count#7730L DESC NULLS LAST], true
             +- Aggregate [actor#7731, actor#6261], [count(1) AS count#7730L, actor#6261, actor#7731]
                +- Project [actor#6261, actor#7731]
                   +- Join Inner, ((movie#6262 = movie#7732) && (actor#6261 < actor#7731))
                      :- Project [actor#6261, movie#6262]
                      :  +- LogicalRDD [actor#6261, movie#6262, year#6263], false
                      +- Project [actor#7731, movie#7732]
                         +- LogicalRDD [actor#7731, movie#7732, year#7733], false

    == Physical Plan ==
    TakeOrderedAndProject(limit=51, orderBy=[count#7730L DESC NULLS LAST], output=[count#7742,actor#6261,actor#7731])
    +- *(6) HashAggregate(keys=[actor#7731, actor#6261], functions=[count(1)], output=[count#7730L, actor#6261, actor#7731])
       +- Exchange hashpartitioning(actor#7731, actor#6261, 200)
          +- *(5) HashAggregate(keys=[actor#7731, actor#6261], functions=[partial_count(1)], output=[actor#7731, actor#6261, count#7749L])
             +- *(5) Project [actor#6261, actor#7731]
                +- *(5) SortMergeJoin [movie#6262], [movie#7732], Inner, (actor#6261 < actor#7731)
                   :- *(2) Sort [movie#6262 ASC NULLS FIRST], false, 0
                   :  +- Exchange hashpartitioning(movie#6262, 200)
                   :     +- *(1) Project [actor#6261, movie#6262]
                   :        +- Scan ExistingRDD[actor#6261,movie#6262,year#6263]
                   +- *(4) Sort [movie#7732 ASC NULLS FIRST], false, 0
                      +- ReusedExchange [actor#7731, movie#7732], Exchange hashpartitioning(movie#6262, 200)
 */
object SparkSQLTaskFour {
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

    spark.sql("select count(m1.movie) as count, m1.actor, m2.actor " +
      "from movies as m1 inner join movies as m2 " +
      "on m1.movie == m2.movie and m1.actor < m2.actor " +
      "group by m2.actor, m1.actor " +
      "order by count desc").show(50)
  }
}
