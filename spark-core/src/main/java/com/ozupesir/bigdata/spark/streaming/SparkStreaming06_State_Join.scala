package com.ozupesir.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming06_State_Join {
  def main(args: Array[String]): Unit = {
    
    // TODO 创建环境对象
    // StreamingContext创建时，需要传递两个参数
    // 第一个参数表示环境配置
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    // 第二个参数表示批量处理的周期（采集周期）
    val ssc = new StreamingContext(sparkConf, Seconds(3))

    val data9999: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)
    val data8888: ReceiverInputDStream[String]  = ssc.socketTextStream("localhost",8888)

    val map9999: DStream[(String, Int)] = data9999.map((_, 9))
    val map8888: DStream[(String, Int)] = data8888.map((_, 8))

    // 所谓的join操作其实就是两个RDD的join操作
    val joinDS: DStream[(String, (Int, Int))] = map9999.join(map8888)

    joinDS.print

    ssc.start()
    ssc.awaitTermination()

  }
}
