package com.spark.learning

import org.apache.log4j.{Level, Logger}
import org.apache.spark.ml.linalg.{Matrix, Vector, Vectors}
import org.apache.spark.ml.stat.Correlation
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.ml.stat.ChiSquareTest
import org.apache.spark.ml.stat.Summarizer

object MLLibSimple {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder.appName("SparkRDDTaskOne")
      .config("spark.master", "local")
      .config("driver-memory", "10G")
      .config("executor-memory", "15G")
      .config("executor-cores", "8")
      .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    import spark.implicits._
    import Summarizer._

    val data1 = Seq(
      Vectors.sparse(4, Seq((0, 1.0), (3, -2.0))),
      Vectors.dense(4.0, 5.0, 0.0, 3.0),
      Vectors.dense(6.0, 7.0, 0.0, 8.0),
      Vectors.sparse(4, Seq((0, 9.0), (3, 1.0)))
    )

    /*
        +--------------------+
        |            features|
        +--------------------+
        |(4,[0,3],[1.0,-2.0])|
        |   [4.0,5.0,0.0,3.0]|
        |   [6.0,7.0,0.0,8.0]|
        | (4,[0,3],[9.0,1.0])|
        +--------------------+
     */
    val df1 = data1.map(Tuple1.apply).toDF("features")

    df1.show()

    /*
        Pearson correlation matrix:
         1.0                   0.055641488407465814  NaN  0.4004714203168137
        0.055641488407465814  1.0                   NaN  0.9135958615342522
        NaN                   NaN                   1.0  NaN
        0.4004714203168137    0.9135958615342522    NaN  1.0
     */
    // https://www.spss-tutorials.com/pearson-correlation-coefficient/
    // https://www.statisticshowto.datasciencecentral.com/probability-and-statistics/correlation-coefficient-formula/
    val Row(coeff1: Matrix) = Correlation.corr(df1, "features").head
    println(s"Pearson correlation matrix:\n $coeff1")

    /*
        Spearman correlation matrix:
         1.0                  0.10540925533894532  NaN  0.40000000000000174
        0.10540925533894532  1.0                  NaN  0.9486832980505141
        NaN                  NaN                  1.0  NaN
        0.40000000000000174  0.9486832980505141   NaN  1.0
     */
    val Row(coeff2: Matrix) = Correlation.corr(df1, "features", "spearman").head
    println(s"Spearman correlation matrix:\n $coeff2")

    val data2 = Seq(
      (0.0, Vectors.dense(0.5, 10.0)),
      (0.0, Vectors.dense(1.5, 20.0)),
      (1.0, Vectors.dense(1.5, 30.0)),
      (0.0, Vectors.dense(3.5, 30.0)),
      (0.0, Vectors.dense(3.5, 40.0)),
      (1.0, Vectors.dense(3.5, 40.0))
    )

    /*
        +-----+----------+
        |label|  features|
        +-----+----------+
        |  0.0|[0.5,10.0]|
        |  0.0|[1.5,20.0]|
        |  1.0|[1.5,30.0]|
        |  0.0|[3.5,30.0]|
        |  0.0|[3.5,40.0]|
        |  1.0|[3.5,40.0]|
        +-----+----------+
     */

    val df2 = data2.toDF("label", "features")

    df2.show()

    /*
        pValues = [0.6872892787909721,0.6822703303362126]
        degreesOfFreedom [2,3]
        statistics [0.75,1.5]
     */
    // https://www.spss-tutorials.com/spss-chi-square-independence-test/
    val chi = ChiSquareTest.test(df2, "features", "label").head
    println(s"pValues = ${chi.getAs[Vector](0)}")
    println(s"degreesOfFreedom ${chi.getSeq[Int](1).mkString("[", ",", "]")}")
    println(s"statistics ${chi.getAs[Vector](2)}")

    val data3 = Seq(
      (Vectors.dense(2.0, 3.0, 5.0), 1.0),
      (Vectors.dense(4.0, 6.0, 7.0), 2.0)
    )

    val df3 = data3.toDF("features", "weight")

    val (meanVal, varianceVal) = df3.select(metrics("mean", "variance")
      .summary($"features", $"weight").as("summary"))
      .select("summary.mean", "summary.variance")
      .as[(Vector, Vector)].first()

    // https://www.spss-tutorials.com/variance-what-is-it/
    // https://www.spss-tutorials.com/how-to-compute-means-in-spss/

    // with weight: mean = [3.333333333333333,5.0,6.333333333333333], variance = [2.000000000000001,4.5,2.000000000000001]
    println(s"with weight: mean = $meanVal, variance = $varianceVal")

    val (meanVal2, varianceVal2) = df3.select(mean($"features"), variance($"features"))
      .as[(Vector, Vector)].first()

    // without weight: mean = [3.0,4.5,6.0], sum = [2.0,4.5,2.0]
    println(s"without weight: mean = $meanVal2, sum = $varianceVal2")

  }
}
