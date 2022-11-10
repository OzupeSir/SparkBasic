package com.ozupesir.bigdata.spark.sql

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object Spark01_SparkSQL_Basic {
  def main(args: Array[String]): Unit = {
    // TODO 创建SparkSQL的运行环境
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkSQL")

    val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

    // TODO 执行操作
    // DataFrame
    //    val df: DataFrame = spark.read.json("datas/user.json")
    //	df.show()

    // DataFrame=>SQL
    //	df.createTempView("user")
    //	spark.sql("select * from user").show

    // DataFrame => DSL
    // 在使用DataFrame时，如果涉及到转换操作，需要引入转换规则
    import spark.implicits._
    //    df.select("age", "username").show
    //    df.select($"age" + 1).show
    //		df.select('age + 1).show 过时方法


    // TODO DataSet

    // DataFrame其实是特定泛型的DataSet
    //    val seq = Seq(1,2,3,4)
    //    val ds: Dataset[Int] = seq.toDS()
    //    ds.show()


    // RDD <=> DataFrame
    val rdd: RDD[(Int, String, Int)] = spark.sparkContext.makeRDD(List((1, "zhangsan", 30), (2, "lisi", 40)))
    val df: DataFrame = rdd.toDF("id", "name", "age")
    val rowRDD: RDD[Row] = df.rdd

    // RDD <=> DataSet
    val ds: Dataset[User] = df.as[User]
    ds.toDF()

    // RDD <=> DataSet
    val ds1: Dataset[User] = rdd.map {
      case (id, name, age) => {
        User(id, name, age)
      }
    }.toDS()
    val userRDD: RDD[User] = ds1.rdd

    // TODO 关闭环境
    spark.close()
  }

  case class User(id: Int, name: String, age: Int)
}
