package com.ozupesir.bigdata.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object Spark22_RDD_Operator_Transform {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    // TODO 算子 - join
    val rdd1: RDD[(String, Int)] = sc.makeRDD(List(
      ("a", 1), ("b", 2), ("c", 3), ("d", 5)
    ))
    val rdd2: RDD[(String, Int)] = sc.makeRDD(List(
      ("a", 4), ("b", 5), ("c", 6), ("a", 7)
    ))

    // join :两个不同数据源的数据，相同的key的value会连接到一起，形成元祖
    //        如果两个数据源中key没有匹配上，name数据不会出现在结果中
    //        如果两个数据源中key有多个相同的，会依次匹配，可能会出现笛卡尔乘积，数据两会几何性的增长，内存存在风险
    val joinRDD1: RDD[(String, (Int, Option[Int]))] = rdd1.leftOuterJoin(rdd2)
    joinRDD1.collect().foreach(println)

    val joinRDD2: RDD[(String, (Option[Int], Int))] = rdd1.rightOuterJoin(rdd2)
    joinRDD2.collect().foreach(println)


    sc.stop()
  }
}
