package com.ozupesir.bigdata.spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext, StreamingContextState}

object SparkStreaming08_Close {
  def main(args: Array[String]): Unit = {

    /*
      线程的关闭
      val thread = new Thread()
      thread.start()

      thread.stop() // 强制关闭
     */

    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
    // TODO 创建环境对象
    // StreamingContext创建时，需要传递两个参数
    // 第一个参数表示环境配置
    // 第二个参数表示批量处理的周期（采集周期）
    val ssc = new StreamingContext(sparkConf, Seconds(3))
    ssc.checkpoint("cp")

    val lines: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)

    val wordToOne: DStream[(String, Int)] = lines.map((_, 1))

    wordToOne.print()

    ssc.start()

    // 如果想要关闭采集器，需要创建一个新的线程
    // 而且需要在第三方程序中增加关闭状态
    new Thread(
      new Runnable {
        override def run(): Unit = {
          // 优雅的关闭
          // 计算节点不再接收新的数据，而是将现有的数据处理完毕，然后关闭
          // exp:
          //  Mysql: Table(stopSpark) => row => data
          //  Redis: Data(K-V)
          //  ZK: /stopSpark
          //  HDFS: /stopSpark
          /*
          while (true) {
            if (true) {
              // 获取SparkStreaming状态
              val state: StreamingContextState = ssc.getState()
              if (state == StreamingContextState.ACTIVE) {
                ssc.stop(true, true)
              }
            }
            Thread.sleep(5000)
          }
          */
          Thread.sleep(5000)
          val state: StreamingContextState = ssc.getState()
          if (state == StreamingContextState.ACTIVE) {
            ssc.stop(true, true)
          }
          System.exit(0)
        }
      }
    )


    ssc.awaitTermination()


  }
}
