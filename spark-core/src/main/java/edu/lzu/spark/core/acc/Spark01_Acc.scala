package edu.lzu.spark.core.acc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_Acc {
    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Acc")
        val sc = new SparkContext(sparkConf)

        val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

        // reduce 分区内计算，分区间计算
        // val sum: Int = rdd.reduce(_ + _)

        var sum: Int = 0    // driver 端的
        rdd.foreach(
            num => {
                sum += num   // Executor 端的
            }
        )

        println(sum)   // 输出的是 Driver端的sum , sum = 0`

        sc.stop()


    }

}
