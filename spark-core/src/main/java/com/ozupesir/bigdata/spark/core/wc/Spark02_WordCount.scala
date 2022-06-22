package com.ozupesir.bigdata.spark.core.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_WordCount {
  def main(args: Array[String]): Unit = {
    //Application

    // Spark框架
    // Todo 建立和Spark框架的连接
    // JDBC : connetcion

    val sparkConf = new SparkConf().setMaster("local").setAppName("WorlCount")
    val sc = new SparkContext(sparkConf)

    // Todo 执行业务作业
    // 1. 读取文件，获取一行一行的数据
    // hello world
    val lines: RDD[String] = sc.textFile(path = "datas")

    // 2. 将一行数据进行拆分，形成一个一个的单词（分词）
    // 扁平化：将整体拆分成个体的操作
    // "hello world"  => hello,world,hello,world
    val words: RDD[String] = lines.flatMap(_.split(" "))

    val wordToOne = words.map((_, 1))

    val wordGroup: RDD[(String, Iterable[(String, Int)])] = wordToOne.groupBy(_._1)

    //    val wordToCount: RDD[(String, Int)] = wordGroup.mapValues(_.size)

    val wordToCount: RDD[(String, Int)] = wordGroup.map {
      case (word, list) => {
        list.reduce(
          (t1, t2) => (word, t1._2 + t2._2)
        )
      }
    }

    val array: Array[(String, Int)] = wordToCount.collect()
    array.foreach(println)
    //  println(array)

    // Todo 关闭连接
    sc.stop()

  }
}
