package com.ozupesir.bigdata.spark.core.rdd.persist

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark04_RDD_Persist {
  def main(args: Array[String]): Unit = {


    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)
    sc.setCheckpointDir("cp")

    val list = List("Hello Scala", "Hello Spark")

    val rdd = sc.makeRDD(list)

    val flatRDD = rdd.flatMap(_.split(" "))

    val mapRDD = flatRDD.map(
      word => {
        println("@@@@@@@@@@@@@@@@@@@")
        (word, 1)
      }
    )

    // checkpoint 需要落盘，需要制定检查点保存路径
    // 检查点路径保存的文件
    //一般保存路径都是在分布式存储系统：HDFS
    mapRDD.checkpoint()

    val reduceRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_ + _)

    reduceRDD.collect().foreach(println)
    println("*" * 20)


    val reduceRDD1: RDD[(String, Iterable[Int])] = mapRDD.groupByKey()

    reduceRDD1.collect().foreach(println)

    sc.stop()

  }
}
