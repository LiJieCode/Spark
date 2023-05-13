package test01

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.dstream.{DStream, InputDStream, ReceiverInputDStream}
import org.apache.spark.streaming.receiver.Receiver
import org.apache.spark.streaming.{Seconds, StreamingContext}


import scala.util.Random

/**
 *
 * 自定义数据源
 *
 */

object demo03 {
    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("demo01")
        // StreamingContext 需要传递两个参数，
        // 第一个参数是环境配置
        // 第二个参数是批量处理的周期（采集周期）
        val ssc = new StreamingContext(sparkConf,Seconds(3))

        val lines: ReceiverInputDStream[String] = ssc.receiverStream(new MyReceiver)

        lines.print()

        ssc.start()
        ssc.awaitTermination()

        // ssc.stop()
    }

    /**
     *
     * 自定义数据采集器
     *
     * 1、自定义类，混入Receiver
     * 2、定义泛型，传入参数
     * 3、定义方法
     *
     */
    class MyReceiver extends Receiver[String](StorageLevel.MEMORY_ONLY){
        private var flag = true
        override def onStart(): Unit = {
            // 创建一个线程，源源不断向ssc输入数据
            new Thread(new Runnable {
                override def run(): Unit = {

                    while (flag){

                        val message: String = "采集的数据为：" + new Random().nextInt(10).toString
                        store(message)
                        Thread.sleep(500)
                    }
                }
            }).start()

        }

        override def onStop(): Unit = {
            flag = false
        }
    }

}
