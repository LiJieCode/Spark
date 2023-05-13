package edu.lzu.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark10_Transform_Test {
    def main(args: Array[String]): Unit = {

        // 连接spark
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("transform")
        val sc = new SparkContext(sparkConf)


        // TODO 算子 - Coalesce
        val rdd: RDD[Int] = sc.makeRDD(
            List(1, 2, 3, 4, 5, 6), 2
        )

        // 扩大分区
        // Coalesce是可以扩大分区的，但是如果不进行shuffle，是没有意义的
        val coalesceRDD: RDD[Int] = rdd.coalesce(3, true)

        coalesceRDD.saveAsTextFile("output")

        // 关闭spark
        sc.stop()

    }
}
