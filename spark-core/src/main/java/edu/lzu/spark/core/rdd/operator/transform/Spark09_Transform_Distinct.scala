package edu.lzu.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark09_Transform_Distinct {
    def main(args: Array[String]): Unit = {

        // 连接spark
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("transform")
        val sc = new SparkContext(sparkConf)


        // TODO 算子 - Distinct
        val rdd: RDD[Int] = sc.makeRDD(
            List(1, 2, 3, 4, 1, 2, 3)
        )

        val distinctRDD: RDD[Int] = rdd.distinct()

        distinctRDD.collect().foreach(println)


        // 关闭spark
        sc.stop()

    }
}
