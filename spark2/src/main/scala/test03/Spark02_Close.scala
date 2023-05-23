package test03

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext, StreamingContextState}

object Spark02_Close {
    def main(args: Array[String]): Unit = {

        /*
           线程的关闭：
           val thread = new Thread()
           thread.start()

           thread.stop(); // 强制关闭

         */

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
        val ssc = new StreamingContext(sparkConf, Seconds(3))

        val lines = ssc.socketTextStream("localhost", 9999)
        val wordToOne = lines.map((_,1))

        wordToOne.print()


        // ssc 开始执行
        ssc.start()


        // 如果想要关闭采集器，那么需要创建新的线程
        // 而且需要在第三方程序中增加关闭状态
        new Thread(
            new Runnable {
                override def run(): Unit = {
                    // 优雅地关闭  ：  使用外部文件系统来控制内部程序的关闭
                    // 计算节点不在接收新的数据，而是将现有的数据处理完毕，然后关闭
                    // Mysql : Table(stopSpark) => Row => data
                    // Redis : Data（K-V）
                    // ZK    : /stopSpark
                    // HDFS  : /stopSpark
                    /*  基础语法  -  真正的代码
                    while ( true ) {
                        if (true) {
                            // 获取SparkStreaming状态
                            val state: StreamingContextState = ssc.getState()
                            if ( state == StreamingContextState.ACTIVE ) {
                                ssc.stop(true, true)
                            }
                        }
                        Thread.sleep(5000)
                    }
                     */

                    // 启动五秒钟后，关闭
                    Thread.sleep(5000)
                    // 获取当前ssc的状态
                    val state: StreamingContextState = ssc.getState()
                    // 如果状态时活跃的，就可以关闭
                    if ( state == StreamingContextState.ACTIVE ) {
                        ssc.stop(true, true)
                    }

                    // 当前线程停止
                    // 当前线程的作用就是关闭资源，一旦关闭，该线程就可以退出了
                    System.exit(0)
                    // 线程当main方法结束了，也会停止
                }
            }
        ).start()


        ssc.awaitTermination()    // block 阻塞main线程


    }
}
