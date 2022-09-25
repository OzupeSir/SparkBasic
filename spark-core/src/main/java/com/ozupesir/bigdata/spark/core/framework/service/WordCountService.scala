package com.ozupesir.bigdata.spark.core.framework.service

import com.ozupesir.bigdata.spark.core.framework.common.TService
import com.ozupesir.bigdata.spark.core.framework.dao.WordCountDao
import org.apache.spark.rdd.RDD

/**
 * 服务层
 */
class WordCountService extends TService{
	private val wordCountDao = new WordCountDao()
	
	// 数据分析
	def dataAnalysis() = {
		
		val lines = wordCountDao.readFile("datas/word.txt")
		
		val words: RDD[String] = lines.flatMap(_.split(" "))
		
		val wordGroup: RDD[(String, Iterable[String])] = words.groupBy(word => word)
		
		val wordToCount: RDD[(String, Int)] = wordGroup.mapValues(_.size)
		
		val array: Array[(String, Int)] = wordToCount.collect()
		array
	}
}
