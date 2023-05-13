package edu.lzu.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark04_Transform_FlatMap1 {
    def main(args: Array[String]): Unit = {

        // 连接spark
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("transform")
        val sc = new SparkContext(sparkConf)


        // TODO 算子 - FlatMap
        val rdd1: RDD[Any] = sc.makeRDD(
            List(List(1, 2), 3, 4, List(5, 6))
        )

        // 利用模式匹配
        val flatRDD = rdd1.flatMap(
            data => {
                data match {
                    case list: List[_] => list
                    case dat => List(dat)
                }
            }
        )


        flatRDD.collect().foreach(println)

        // 关闭spark
        sc.stop()

    }
}
