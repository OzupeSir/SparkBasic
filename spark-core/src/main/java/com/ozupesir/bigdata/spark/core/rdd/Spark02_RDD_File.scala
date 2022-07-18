package com.ozupesir.bigdata.spark.core.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_File {
  def main(args: Array[String]): Unit = {

    // TODO 准备环境
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
    val sc = new SparkContext(sparkConf)

    // TODO 创建RDD
    // 从文件中创建RDD，将文件中集合得数据作为数处理得数据源


    // PATH路径默认以当前环境得根路径为基准，可以写绝对路径，也可以写相对路径
    //    val rdd: RDD[String] = sc.textFile("datas/1.txt")

    // PATH路径可以是文件的具体路径，也可以是目录名称
    //    val rdd: RDD[String] = sc.textFile("datas/*")

    // PATH路径可以使用通配符
    //    val rdd: RDD[String] = sc.textFile("datas/1*.txt")

    // path还可以是分布式存储系统路径：HDFS
    val rdd: RDD[String] = sc.textFile("hdfs://linux:8020/datas/1*.txt")

    rdd.collect().foreach(println)

    // TODO 关闭环境
    sc.stop()
  }
}
