package edu.lzu.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark04_Transform_FlatMap2 {
    def main(args: Array[String]): Unit = {

        // 连接spark
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("transform")
        val sc = new SparkContext(sparkConf)


        // TODO 算子 - FlatMap
        val rdd1: RDD[String] = sc.makeRDD(
            List("hell word", "hello spark")
        )

        val flatRDD = rdd1.flatMap(
            s => {
                s.split(" ")

            }
        )


        flatRDD.collect().foreach(println)

        // 关闭spark
        sc.stop()

    }
}
