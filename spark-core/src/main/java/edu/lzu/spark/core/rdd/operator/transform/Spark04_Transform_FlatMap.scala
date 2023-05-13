package edu.lzu.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark04_Transform_FlatMap {
    def main(args: Array[String]): Unit = {

        // 连接spark
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("transform")
        val sc = new SparkContext(sparkConf)


        // TODO 算子 - FlatMap
        // 将处理的数据进行扁平化后再进行映射处理，所以算子也称之为扁平映射
        val rdd1: RDD[List[Int]] = sc.makeRDD(
            List(List(1, 2),  List(5, 6))
        )

        val flatRDD: RDD[Int] = rdd1.flatMap(
            list => {
                list
            }
        )

        flatRDD.collect().foreach(println)

        // 关闭spark
        sc.stop()

    }
}
