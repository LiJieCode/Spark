package test03

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

object Spark03_Resume {
    def main(args: Array[String]): Unit = {

        // 从检查点恢复数据
        val ssc = StreamingContext.getActiveOrCreate("cp", ()=>{
            val sparkConf = new SparkConf().setMaster("local[*]").setAppName("SparkStreaming")
            val ssc = new StreamingContext(sparkConf, Seconds(3))

            val lines = ssc.socketTextStream("localhost", 9999)
            val wordToOne = lines.map((_,1))

            wordToOne.print()

            ssc
        })

        // 配置检查点目录
        ssc.checkpoint("cp")



        ssc.start()
        ssc.awaitTermination() // block 阻塞main线程

    }

}
