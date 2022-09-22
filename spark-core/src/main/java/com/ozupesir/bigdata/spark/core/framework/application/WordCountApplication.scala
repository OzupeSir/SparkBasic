package com.ozupesir.bigdata.spark.core.framework.application

import com.ozupesir.bigdata.spark.core.framework.controller.WordCountController
import org.apache.spark.{SparkConf, SparkContext}

object WordCountApplication extends App{
	
	val sparkConf = new SparkConf().setMaster("local[*]").setAppName("RDD")
	val sc = new SparkContext(sparkConf)
	
	val wordCountController = new WordCountController()
	
	// TODO 关闭链接
	sc.stop()
}
