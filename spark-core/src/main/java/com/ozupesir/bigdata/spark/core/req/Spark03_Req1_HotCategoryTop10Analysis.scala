package com.ozupesir.bigdata.spark.core.req

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark03_Req1_HotCategoryTop10Analysis {
	
	def main(args: Array[String]): Unit = {
		// TODO:TOP10热门品类
		val sparkConf:SparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
		val sc = new SparkContext(sparkConf)
		
		// Q:存在大量的shuffle操作（reduceByKey）
		// reduceByKey 聚合算子，spark会提供优化，缓存操作
		
		// 1. 读取原始的日志数据
		val actionRDD: RDD[String] = sc.textFile("datas/user_visit_action.txt")
		
		// 2. 将数据转换结构
		// 点击的场合：（品类ID,(1,0,0)）
		// 下单的场合：（品类ID,(0,1,0)）
		// 支付的场合：（品类ID,(0,0,1)）
		val flatRDD: RDD[(String,(Int,Int,Int))] = actionRDD.flatMap(
			action => {
				val datas: Array[String] = action.split("_")
				if (datas(6) != "-1") {
					// 点击的场合
					List((datas(6), (1, 0, 0)))
				} else if (datas(8) != "null") {
					// 下单的场合
					val ids: Array[String] = datas(8).split(",")
					ids.map(id => (id, (0, 1, 0)))
				} else if (datas(10) != "null") {
					// 下单的场合
					val ids: Array[String] = datas(10).split(",")
					ids.map(id => (id, (0, 0, 1)))
				} else {
					Nil
				}
			}
		)
		
		// 3.将相同品类ID的数据进行分组聚合
		//	（品类ID，（点击数量，下单数量，支付数量））
		val analysisRDD: RDD[(String, (Int, Int, Int))] = flatRDD.reduceByKey(
			(value1, value2) => {
				(value1._1 + value2._1, value1._2 + value2._2, value1._3 + value2._3)
			}
		)
		
		// 4. 将统计结果
		val resultRDD: Array[(String, (Int, Int, Int))] = analysisRDD.sortBy(_._2, false).take(10)
		
		// \. 将结果采集到控制台打印出来
		resultRDD.foreach(println)
		
	}
}
