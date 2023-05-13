package test01

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 *
 * WordCount
 *
 */
object demo01 {
    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("demo01")
        // StreamingContext 需要传递两个参数，
        // 第一个参数是环境配置
        // 第二个参数是批量处理的周期（采集周期）
        val ssc = new StreamingContext(sparkConf,Seconds(3))

        // 获取端口数据
        val lines: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 7777)

        val words: DStream[String] = lines.flatMap(_.split(" "))

        val wordToOne: DStream[(String, Int)] = words.map((_, 1))

        val wc: DStream[(String, Int)] = wordToOne.reduceByKey(_ + _)

        wc.print()


        //
        ssc.start()
        ssc.awaitTermination()

        // ssc.stop()
    }

}
