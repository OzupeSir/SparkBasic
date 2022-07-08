package com.ozupesir.bigdata.spark.core.test

import java.io.{ObjectOutputStream, OutputStream}
import java.net.Socket

object Driver {
  def main(args: Array[String]): Unit = {
    // 连接服务器

    //    val client = new Socket("localhost", 8888)


    val client1 = new Socket("localhost", 9999)
    val client2 = new Socket("localhost", 8888)


    /*
    val out: OutputStream = client.getOutputStream

    out.write(2)
    out.flush()
    out.close()

    client.close()
     */

    /*
        val out: OutputStream = client.getOutputStream
        val objOut: ObjectOutputStream = new ObjectOutputStream(out)
    */


    val task = new Task()

    val out1: OutputStream = client1.getOutputStream
    val objOut1: ObjectOutputStream = new ObjectOutputStream(out1)

    val subTask1 = new SubTask()
    subTask1.logic = task.logic
    subTask1.datas = task.datas.take(2)

    objOut1.writeObject(subTask1)
    objOut1.flush()
    objOut1.close()
    client1.close()

    val out2: OutputStream = client2.getOutputStream
    val objOut2: ObjectOutputStream = new ObjectOutputStream(out2)

    val subTask2 = new SubTask()
    subTask2.logic = task.logic
    subTask2.datas = task.datas.takeRight(2)

    objOut2.writeObject(subTask2)
    objOut2.flush()
    objOut2.close()
    objOut2.close()
    println("客户端数据发送完毕")

  }
}
