package test02

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Demo001 {
    def main(args: Array[String]): Unit = {
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("0511")

        val scc: StreamingContext = new StreamingContext(sparkConf, Seconds(3))



    }
}
