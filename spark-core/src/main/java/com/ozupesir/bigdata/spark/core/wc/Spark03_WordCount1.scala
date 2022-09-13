package com.ozupesir.bigdata.spark.core.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Spark03_WordCount1 {
  def main(args: Array[String]): Unit = {
    //Application

    // Spark框架
    // Todo 建立和Spark框架的连接
    // JDBC : connetcion

    val sparkConf = new SparkConf().setMaster("local").setAppName("WorlCount")
    val sc = new SparkContext(sparkConf)

    wordCount11(sc)


    sc.stop()

  }

  def wordCount1(sc: SparkContext): Unit = {
    val rdd = sc.makeRDD(List("Hello Scala", "Hello Spark"))
    val words = rdd.flatMap(_.split(" "))
    val group: RDD[(String, Iterable[String])] = words.groupBy(word => word)
    val wordCount: RDD[(String, Int)] = group.mapValues(iter => iter.size)
  }

  def wordCount2(sc: SparkContext): Unit = {
    val rdd = sc.makeRDD(List("Hello Scala", "Hello Spark"))
    val words = rdd.flatMap(_.split(" "))
    val wordOne: RDD[(String, Int)] = words.map((_, 1))
    val group: RDD[(String, Iterable[Int])] = wordOne.groupByKey()
    val wordCount: RDD[(String, Int)] = group.mapValues(iter => iter.size)
  }

  // reduceByKey
  def wordCount3(sc: SparkContext): Unit = {
    val rdd = sc.makeRDD(List("Hello Scala", "Hello Spark"))
    val words = rdd.flatMap(_.split(" "))
    val wordOne: RDD[(String, Int)] = words.map((_, 1))
    val wordCount: RDD[(String, Int)] = wordOne.reduceByKey(_ + _)
  }

  // aggregateByKey
  def wordCount4(sc: SparkContext): Unit = {
    val rdd = sc.makeRDD(List("Hello Scala", "Hello Spark"))
    val words = rdd.flatMap(_.split(" "))
    val wordOne: RDD[(String, Int)] = words.map((_, 1))
    val wordCount: RDD[(String, Int)] = wordOne.aggregateByKey(0)(_ + _, _ + _)
  }

  // foldByKey
  def wordCount5(sc: SparkContext): Unit = {
    val rdd = sc.makeRDD(List("Hello Scala", "Hello Spark"))
    val words = rdd.flatMap(_.split(" "))
    val wordOne: RDD[(String, Int)] = words.map((_, 1))
    val wordCount: RDD[(String, Int)] = wordOne.foldByKey(0)(_ + _)
  }

  // combineByKey
  def wordCount6(sc: SparkContext): Unit = {
    val rdd = sc.makeRDD(List("Hello Scala", "Hello Spark"))
    val words = rdd.flatMap(_.split(" "))
    val wordOne: RDD[(String, Int)] = words.map((_, 1))
    val wordCount: RDD[(String, Int)] = wordOne.combineByKey(
      v => v,
      (x, y) => x + y,
      (x, y) => x + y
    )
  }

  // countByKey
  def wordCount7(sc: SparkContext): Unit = {
    val rdd = sc.makeRDD(List("Hello Scala", "Hello Spark"))
    val words = rdd.flatMap(_.split(" "))
    val wordOne: RDD[(String, Int)] = words.map((_, 1))
    val wordCount: collection.Map[String, Long] = wordOne.countByKey()
  }

  // countByValue
  def wordCount8(sc: SparkContext): Unit = {
    val rdd = sc.makeRDD(List("Hello Scala", "Hello Spark"))
    val words = rdd.flatMap(_.split(" "))
    val wordCount: collection.Map[String, Long] = words.countByValue()
  }

  // reduce
  def wordCount9(sc: SparkContext): Unit = {
    val rdd = sc.makeRDD(List("Hello Scala", "Hello Spark"))
    val words = rdd.flatMap(_.split(" "))

    // 【（word，count），（word，count）】
    //  word => Map【(word,1)】

    val mapWord: RDD[mutable.Map[String, Long]] = words.map(
      word => {
        mutable.Map[String, Long]((word, 1))
      }
    )
    val wordCount: mutable.Map[String, Long] = mapWord.reduce(
      (map1, map2) => {
        map2.foreach {
          case (word, count) =>
            val newCount = map1.getOrElse(word, 0L) + count
            map1.update(word, newCount)
        }
        map1
      }
    )

    println(wordCount)
  }

  // aggregate
  def wordCount10(sc: SparkContext): Unit = {
    val rdd = sc.makeRDD(List("Hello Scala", "Hello Spark"))
    val words = rdd.flatMap(_.split(" "))

    // 【（word，count），（word，count）】
    //  word => Map【(word,1)】

    val mapWord: RDD[mutable.Map[String, Long]] = words.map(
      word => {
        mutable.Map[String, Long]((word, 1))
      }
    )

    val wordCount: mutable.Map[String, Long] = mapWord.aggregate(mutable.Map[String, Long]())(
      (map1, map2) => {
        map2.foreach {
          case (word, count) =>
            val newCount = map1.getOrElse(word, 0L) + count
            map1.update(word, newCount)
        }
        map1
      },
      (map1, map2) => {
        map2.foreach {
          case (word, count) =>
            val newCount = map1.getOrElse(word, 0L) + count
            map1.update(word, newCount)
        }
        map1
      }
    )

    println(wordCount)
  }

  // fold
  def wordCount11(sc: SparkContext): Unit = {
    val rdd = sc.makeRDD(List("Hello Scala", "Hello Spark"))
    val words = rdd.flatMap(_.split(" "))

    // 【（word，count），（word，count）】
    //  word => Map【(word,1)】

    val mapWord: RDD[mutable.Map[String, Long]] = words.map(
      word => {
        mutable.Map[String, Long]((word, 1))
      }
    )

    val wordCount: mutable.Map[String, Long] = mapWord.fold(mutable.Map[String, Long]())(
      (map1, map2) => {
        map2.foreach {
          case (word, count) =>
            val newCount = map1.getOrElse(word, 0L) + count
            map1.update(word, newCount)
        }
        map1
      }
    )

    println(wordCount)
  }
}
