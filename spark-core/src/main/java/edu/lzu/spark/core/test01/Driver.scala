package edu.lzu.spark.core.test01

import java.io.OutputStream
import java.net.Socket

object Driver {
  def main(args: Array[String]): Unit = {

    // 连接服务器，产生一个客服端
    val client: Socket = new Socket("localhost", 9999)

    // 输出流，向服务器输出数据
    val out: OutputStream = client.getOutputStream

    out.write(2)

    out.flush()
    out.close()
    client.close()
  }

}
