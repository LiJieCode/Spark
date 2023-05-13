package test01

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

/**
 *
 * 通过Queue, 创建DStream
 *
 */

object demo02 {
    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("demo01")
        // StreamingContext 需要传递两个参数，
        // 第一个参数是环境配置
        // 第二个参数是批量处理的周期（采集周期）
        val ssc = new StreamingContext(sparkConf,Seconds(3))

        // RDD 队列
        val rddQueue: mutable.Queue[RDD[Int]] = new mutable.Queue[RDD[Int]]()

        val inputStream: InputDStream[Int] = ssc.queueStream(rddQueue, oneAtATime = false)

        val mapStream: DStream[(Int, Int)] = inputStream.map((_, 1))

        val wc: DStream[(Int, Int)] = mapStream.reduceByKey(_ + _)

        wc.print()

        ssc.start()

        for (i <- 1 to 5){
            rddQueue += ssc.sparkContext.makeRDD(1 to 300, 10)
            Thread.sleep(2000)
        }

        ssc.awaitTermination()

        // ssc.stop()
    }

}
