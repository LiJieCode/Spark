package exercise

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.collection.mutable

object Demo01 {
    def main(args: Array[String]): Unit = {
        val sparkConf: SparkConf = new SparkConf().setMaster("local").setAppName("0513")

        // StreamingContext 需要传递两个参数，
        // 第一个参数是环境配置
        // 第二个参数是批量处理的周期（采集周期）
        val ssc = new StreamingContext(sparkConf, Seconds(3))

        // 创建一个RDD
        val rddQueue: mutable.Queue[RDD[Int]] = new mutable.Queue[RDD[Int]]()

        // 创建一个 QueueInputDStream
        //
        val inputStream: InputDStream[Int] = ssc.queueStream(rddQueue, oneAtATime = false)

        val mapDStream: DStream[(Int, Int)] = inputStream.map((_, 1))

        val wc: DStream[(Int, Int)] = mapDStream.reduceByKey(_ + _)

        wc.print()

        ssc.start()
        // 创建一个线程
        for(i <- 1 to 5){
            rddQueue += ssc.sparkContext.makeRDD(1 to 300, 5)
            Thread.sleep(1000)
        }

        ssc.awaitTermination()
    }

}
