package com.ozupesir.bigdata.spark.core.rdd.operator.action

import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_Operator_Action {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Operator")
    val sc = new SparkContext(sparkConf)

    val rdd = sc.makeRDD(List(1, 3, 2, 4))

    // TODO - 行动算子
    // reduce
    val i: Int = rdd.reduce(_ + _)
    println(s"reduce:${i}")

    // collect：方法会将不同分区的数据按照分区顺序采集到Driver端内存中，形成数组
    val ints: Array[Int] = rdd.collect()
    val intsMkString = ints.mkString(",")
    println(s"collect：${intsMkString}")

    // count ：数据源中数据的个数
    val cnt: Long = rdd.count()
    println(s"count:${cnt}")

    // first ：获取数据源中数据的第一个
    val first: Int = rdd.first()
    println(s"first:${first}")

    // take：获取N个数据
    val takeInts: Array[Int] = rdd.take(3)
    val takeIntsMkString = takeInts.mkString(",")
    println(s"take:${takeIntsMkString}")

    // takeOrdered ：数据排序后，取N个数据
    val takeOrderdInts: Array[Int] = rdd.takeOrdered(3)
    val takeOrderdIntsMkString: String = takeOrderdInts.mkString(",")
    println(s"takeOrderd:${takeOrderdIntsMkString}")

    // aggregate

    sc.stop()
  }
}
