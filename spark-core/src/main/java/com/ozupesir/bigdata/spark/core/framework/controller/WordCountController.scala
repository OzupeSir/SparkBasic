package com.ozupesir.bigdata.spark.core.framework.controller

import com.ozupesir.bigdata.spark.core.framework.common.TController
import com.ozupesir.bigdata.spark.core.framework.service.WordCountService

/**
 * 控制层
 */
class WordCountController extends TController{
	private val wordCountService = new WordCountService()
	
	// 调度
	def dispatch(): Unit = {
		val array: Array[(String, Int)] = wordCountService.dataAnalysis()
		
		array.foreach(println)
	}
}
