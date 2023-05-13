package edu.lzu.spark.core.test01

import java.io.InputStream
import java.net.{ServerSocket, Socket}

object Executor1 {
  def main(args: Array[String]): Unit = {

    // 启动服务器，接收数据
    val server: ServerSocket = new ServerSocket(9999)
    println("服务器启动，等待接收数据。。。。。。")

    // 等待客户端的连接
    // 客户端连接成功后，服务端可以接收到一个服务器对象
    // 如果没有客户端连接，当前线程就会阻塞。。。
    val client: Socket = server.accept()

    // 接收数据，得到一个输入流
    val in: InputStream = client.getInputStream

    val i = in.read()
    println("接收到客户端发送的数据: " + i)

    in.close()
    client.close()
    server.close()
  }

}
