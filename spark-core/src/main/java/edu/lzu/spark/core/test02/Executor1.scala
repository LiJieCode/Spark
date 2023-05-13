package edu.lzu.spark.core.test02

import java.io.{InputStream, ObjectInputStream}
import java.net.{ServerSocket, Socket}

// Executor 主要任务是计算，所以 数据 和 计算方式 都需要从Driver传进来，不能写死
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
        val objIn: ObjectInputStream = new ObjectInputStream(in)

        // 接收传过来的对象
        val task: Task = objIn.readObject().asInstanceOf[Task]

        // 计算
        val ints: List[Int] = task.compute()

        println("计算节点计算的结果为 " + ints)

        // 关闭
        objIn.close()
        client.close()
        server.close()
    }

}
