package com.ozupesir.bigdata.spark.core.req

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark05_Req2_HotCategoryTop10SessionAnalysis {
	
	def main(args: Array[String]): Unit = {
		// TODO:TOP10热门品类
		val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
		val sc = new SparkContext(sparkConf)
		
		// Q:存在大量的shuffle操作（reduceByKey）
		// reduceByKey 聚合算子，spark会提供优化，缓存操作
		
		val actionRDD = sc.textFile("datas/user_visit_action.txt")
		
		val top10Ids: Array[String] = top10Category(actionRDD)
		
		// 1.过滤原始数据，保留点击和前10品类ID
		val filterActionRDD: RDD[String] = actionRDD.filter(
			action => {
				val datas = action.split("_")
				if (datas(6) != "-1") {
					top10Ids.contains(datas(6))
				} else {
					false
				}
			}
		)
		
		// 2. 根据品类ID和Sessionid进行点击量的统计
		val reduceRDD: RDD[((String, String), Int)] = filterActionRDD.map(
			action => {
				val datas = action.split("_")
				((datas(6), datas(2)), 1)
			}
		).reduceByKey(_ + _)
		
		// 3. 将统计的结果进行结构的转换
		// ((品类ID，sessionId),sum)=>(品类ID，（sessionId,sum）)
		val mapRDD: RDD[(String, (String, Int))] = reduceRDD.map {
			case ((cid, sid), sum) => {
				(cid, (sid, sum))
			}
		}
		
		// 4.相同的品类进行分组
		val groupRDD: RDD[(String, Iterable[(String, Int)])] = mapRDD.groupByKey()
		
		// 5. 将分组后的数据进行点击量的排序，取前10名
		val resultRDD: RDD[(String, List[(String, Int)])] = groupRDD.mapValues(
			iter => {
				iter.toList.sortBy(_._2)(Ordering.Int.reverse).take(10)
			}
		)
		resultRDD.collect().foreach(println)
		
		sc.stop()
		
	}
	
	def top10Category(actionRDD: RDD[String]) = {
		val flatRDD: RDD[(String, (Int, Int, Int))] = actionRDD.flatMap(
			action => {
				val datas = action.split("_")
				if (datas(6) != "-1") {
					// 点击的场合
					List((datas(6), (1, 0, 0)))
				} else if (datas(8) != "null") {
					// 下单的场合
					val ids = datas(8).split(",")
					ids.map(id => (id, (0, 1, 0)))
				} else if (datas(10) != "null") {
					// 下单的场合
					val ids = datas(10).split(",")
					ids.map(id => (id, (0, 0, 1)))
				} else {
					Nil
				}
			}
		)
		
		val analysisRDD: RDD[(String, (Int, Int, Int))] = flatRDD.reduceByKey(
			(value1, value2) => {
				(value1._1 + value2._1, value1._2 + value2._2, value1._3 + value2._3)
			}
		)
		
		// 4. 将统计结果
		analysisRDD.sortBy(_._2, false).take(10).map(_._1)
	}
}
