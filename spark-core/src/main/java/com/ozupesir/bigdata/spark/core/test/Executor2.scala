package com.ozupesir.bigdata.spark.core.test

import java.io.{InputStream, ObjectInputStream}
import java.net.{ServerSocket, Socket}

object Executor2 {
  def main(args: Array[String]): Unit = {
    // 启动服务器，结收数据
    val server = new ServerSocket(8888)
    println("服务器启动，等待结收数据")

    // 等待客户端连接
    val client: Socket = server.accept()
    val in: InputStream = client.getInputStream
    /*
        val i: Int = in.read()
        println("接收到客户端发送的数据： " + i)

     */

    val objIn = new ObjectInputStream(in)
    val task: SubTask = objIn.readObject().asInstanceOf[SubTask]
    val ints: List[Int] = task.compute()
    println("计算节点计算的结果为： " + ints)

    objIn.close()
    client.close()
    server.close()
  }
}
