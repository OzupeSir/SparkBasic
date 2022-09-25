package com.ozupesir.bigdata.spark.core.framework.common

import com.ozupesir.bigdata.spark.core.framework.util.EnvUtil

trait TDao {
	def readFile(path: String) = {
		EnvUtil.take().textFile(path = path)
	}
}
