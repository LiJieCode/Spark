package edu.lzu.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_Transform_Partition {
    def main(args: Array[String]): Unit = {

        // 连接spark
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("transform")
        val sc = new SparkContext(sparkConf)


        // TODO 算子 - Map
        // 注意：local[*] 和 local 的区别
        // 如果不指定分区个数，默认是自己电脑的核数（逻辑处理器个数）
        // 指定分区就按照分区个数，进行分配
        // 要去了解，分区分配的底层原理？？？？？？？？？？？？？？？？？？？？？？？？？？？？？？？？？？？？？？？？？？？？
        val rdd1: RDD[Int] = sc.makeRDD(
            List(1, 2, 3, 4)
        )

        // 演示，每个分区独立进行数据的处理
        val mapRDD: RDD[Int] = rdd1.map(
            num => num * 2
        )

        mapRDD.saveAsTextFile("output")


        // 关闭spark
        sc.stop()

    }
}
