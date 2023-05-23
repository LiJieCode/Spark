package test02

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}


/**
 * 转换
 *
 * 无状态转换操作
 *
 */

object Spark01_Wu {
    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("0511")
        val ssc: StreamingContext = new StreamingContext(sparkConf, Seconds(3))


        val data: ReceiverInputDStream[String] = ssc.socketTextStream("localhost", 9999)

        val oneToOne: DStream[(String, Int)] = data.map((_, 1))

        val wc: DStream[(String, Int)] = oneToOne.reduceByKey(_ + _)

        wc.print()

        ssc.start()
        ssc.awaitTermination()
    }
}
