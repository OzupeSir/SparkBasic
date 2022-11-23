package com.ozupesir.bigdata.spark.streaming

import org.apache.spark.{SPARK_BRANCH, SparkConf}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaming06_State_Window {
  def main(args: Array[String]): Unit = {

    // TODO 创建环境对象
    // StreamingContext创建时，需要传递两个参数
    // 第一个参数表示环境配置
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")

    // 第二个参数表示批量处理的周期（采集周期）

    val ssc = new StreamingContext(sparkConf, Seconds(3))
    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)

    val wordToOne: DStream[(String, Int)] = lines.map((_, 1))

    // 窗口的范围应该是采集周期的整数倍
    // 窗口是可以华东的，但是默认情况下，一个采集周期进行滑动
    // 这样的话，可能会出现重复数据的计算，为了避免这种情况，可以改变滑动的步长
    val windowsDS: DStream[(String, Int)] = wordToOne.window(Seconds(6), Seconds(6))

    val wordToCount: DStream[(String, Int)] = windowsDS.reduceByKey(_ + _)

    wordToCount.print()

    ssc.start()
    ssc.awaitTermination()

  }
}
