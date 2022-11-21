package com.ozupesir.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.Aggregator

object Spark04_SparkSQL_JDBC {
  def main(args: Array[String]): Unit = {
    // TODO 创建SparkSQL的运行环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL")
    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    import spark.implicits._

    // 读取Mysql数据
    val df: DataFrame = spark.read
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/spark-sql")
      .option("user", "root")
      .option("password", "zjp04213118.")
      .option("dbtable", "user")
      .load()

    df.show

    df.write
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/spark-sql")
      .option("user", "root")
      .option("password", "zjp04213118.")
      .option("dbtable", "user1")
      .mode(SaveMode.Append)
      .save()

    // TODO 关闭环境
    spark.close()
  }
}
