package edu.lzu.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark03_Transform_MapPartitionsWithIndex {
    def main(args: Array[String]): Unit = {

        // 连接spark
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("transform")
        val sc = new SparkContext(sparkConf)

        // TODO 算子 - MapPartitionsWithIndex
        // 将待处理的数据以分区为单位发送到计算节点进行处理，这里的处理是指可以进行任意的处理，哪怕是过滤数据，在处理时同时可以获取当前分区索引
        val rdd1: RDD[Int] = sc.makeRDD(
            List(1, 2, 3, 4), 2
        )

        // 要求： 保留第二个分区的数据
        val mapRDD = rdd1.mapPartitionsWithIndex(
            (index, iter) => {
                if (index == 1) iter
                else Nil.iterator
            }
        )


        mapRDD.collect().foreach(println)

        // 关闭spark
        sc.stop()

    }
}
