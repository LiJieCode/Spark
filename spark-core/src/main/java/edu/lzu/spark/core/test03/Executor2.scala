package edu.lzu.spark.core.test03

import java.io.{InputStream, ObjectInputStream}
import java.net.{ServerSocket, Socket}

object Executor2 {
    def main(args: Array[String]): Unit = {

        // 启动服务器，接收数据
        val server: ServerSocket = new ServerSocket(8888)
        println("服务器启动，等待接收数据。。。。。。")

        // 等待客户端的连接
        // 客户端连接成功后，服务端可以接收到一个服务器对象
        // 如果没有客户端连接，当前线程就会阻塞。。。
        val client: Socket = server.accept()

        // 接收数据，得到一个输入流
        val in: InputStream = client.getInputStream
        val objIn: ObjectInputStream = new ObjectInputStream(in)

        // 接收传过来的对象
        val task: SubTask = objIn.readObject().asInstanceOf[SubTask]

        // 计算
        val ints: List[Int] = task.compute()

        println("计算节点[8888]计算的结果为 " + ints)

        // 关闭
        objIn.close()
        client.close()
        server.close()
    }

}
