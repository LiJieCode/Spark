package edu.lzu.spark.core.test02

import java.io.{ObjectOutputStream, OutputStream}
import java.net.Socket

object Driver {
    def main(args: Array[String]): Unit = {

        // 连接服务器
        val client = new Socket("localhost", 9999)

        // 输出流，向服务器输出数据
        val out: OutputStream = client.getOutputStream
        val objOut: ObjectOutputStream = new ObjectOutputStream(out)

        val task: Task = new Task

        objOut.writeObject(task)

        objOut.flush()
        // 只需要关闭外层的流
        objOut.close()
        client.close()
        println("客户端数据发送完毕！！")
    }

}
