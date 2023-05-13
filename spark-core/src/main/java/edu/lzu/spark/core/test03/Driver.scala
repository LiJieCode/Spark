package edu.lzu.spark.core.test03

import java.io.{ObjectOutputStream, OutputStream}
import java.net.Socket

object Driver {
    def main(args: Array[String]): Unit = {

        // 连接服务器
        val client1 = new Socket("localhost", 9999)
        val client2 = new Socket("localhost", 8888)

        val task: Task = new Task

        // 输出流，向服务器输出数据
        val out1: OutputStream = client1.getOutputStream
        val objOut1: ObjectOutputStream = new ObjectOutputStream(out1)

        val task1: SubTask = new SubTask
        // 定义数据
        task1.datas = task.datas.take(2)
        // 定义逻辑
        task1.logic = task.logic
        // 传（输出）给服务器
        objOut1.writeObject(task1)
        // 输出流一定要刷新
        objOut1.flush()
        // 只需要关闭外层的流
        objOut1.close()
        client1.close()

        /* ---------------------------------------------------------- */

        // 输出流，向服务器输出数据
        val out2: OutputStream = client2.getOutputStream
        val objOut2: ObjectOutputStream = new ObjectOutputStream(out2)

        val task2: SubTask = new SubTask
        // 定义数据
        task2.datas = task.datas.takeRight(2)
        // 定义逻辑
        task2.logic = task.logic
        // 传（输出）给服务器
        objOut2.writeObject(task2)

        objOut2.flush()
        // 只需要关闭外层的流
        objOut2.close()
        client2.close()

        println("客户端数据发送完毕！！")
    }

}
