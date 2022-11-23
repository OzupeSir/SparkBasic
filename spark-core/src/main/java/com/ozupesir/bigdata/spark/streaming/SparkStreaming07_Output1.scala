package com.ozupesir.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming07_Output1 {
  def main(args: Array[String]): Unit = {

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    // TODO 创建环境对象
    // StreamingContext创建时，需要传递两个参数
    // 第一个参数表示环境配置
    // 第二个参数表示批量处理的周期（采集周期）
    val ssc = new StreamingContext(sparkConf, Seconds(3))
    ssc.checkpoint("cp")

    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)

    val wordToOne: DStream[(String, Int)] = lines.map((_, 1))


    val windowsDS: DStream[(String, Int)] = wordToOne.reduceByKeyAndWindow(
      (x: Int, y: Int) => x + y,
      (x: Int, y: Int) => x - y,
      Seconds(9),
      Seconds(3))

    // foreachRDD不会出现时间戳
    windowsDS.foreachRDD(
      rdd=> {
        rdd
      }
    )

    ssc.start()
    ssc.awaitTermination()

  }
}
