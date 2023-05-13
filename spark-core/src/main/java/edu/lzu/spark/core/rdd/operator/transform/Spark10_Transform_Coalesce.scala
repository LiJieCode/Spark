package edu.lzu.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark10_Transform_Coalesce {
    def main(args: Array[String]): Unit = {

        // 连接spark
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("transform")
        val sc = new SparkContext(sparkConf)


        // TODO 算子 - Coalesce
        val rdd: RDD[Int] = sc.makeRDD(
            List(1, 2, 3, 4, 5, 6), 3
        )

        // val coalesceRDD: RDD[Int] = rdd.coalesce(2)
        // 结果为
        // 0分区：1 2
        // 1分区：3 4 5 6
        // 原因是，Coalesce缩减分区，默认情况，不会打乱分区。所以这种操作可能会导致数据不均衡，出现数据倾斜
        //       如果想让数据均衡，可以进行shuffle处理（打乱重写处理）

        // coalesce的第二个参数，默认false，不进行shuffle
        val coalesceRDD: RDD[Int] = rdd.coalesce(2, true)


        coalesceRDD.saveAsTextFile("output")

        // 关闭spark
        sc.stop()

    }
}
