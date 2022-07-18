package com.ozupesir.bigdata.spark.core.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_File1 {
  def main(args: Array[String]): Unit = {

    // TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    // TODO 创建RDD
    // 从文件中创建RDD，将文件中集合得数据作为数处理得数据源

    // textFile: 以行为单位来读取数据
    // wholeTextFile: 以文件为单位读取数据
    // 读取的结果表示为元祖，第一个元素表示文件路径，第二个元素表示文件内容
    val rdd = sc.wholeTextFiles("datas")
    rdd.collect().foreach(println)


    // TODO 关闭环境
    sc.stop()
  }
}
