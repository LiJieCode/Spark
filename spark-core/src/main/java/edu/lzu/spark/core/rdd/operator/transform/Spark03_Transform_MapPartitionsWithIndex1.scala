package edu.lzu.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark03_Transform_MapPartitionsWithIndex1 {
    def main(args: Array[String]): Unit = {

        // 连接spark
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("transform")
        val sc = new SparkContext(sparkConf)

        // TODO 算子 - MapPartitionsWithIndex
        val rdd1: RDD[Int] = sc.makeRDD(
            List(1, 2, 3, 4)
        )

        // 要求： 显示每个数据所在的分区
        val mapRDD: RDD[(Int, Int)] = rdd1.mapPartitionsWithIndex(
            (index, iter) => {
                iter.map((_, index))
            }
        )




        /*val mapRDD = rdd1.mapPartitionsWithIndex(
            (index, iter) => {
                iter.map(
                    num => {
                        (index, num)
                    }
                )
            }
        )*/


        mapRDD.collect().foreach(println)

        // 关闭spark
        sc.stop()

    }
}
