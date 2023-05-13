package edu.lzu.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark20_Transform_SortByKey {
    def main(args: Array[String]): Unit = {

        // 连接spark
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("transform")
        val sc = new SparkContext(sparkConf)


        // TODO 算子 - SortByKey
        val rdd: RDD[(String, Int)] = sc.makeRDD(
            List(
                ("a", 1), ("a", 2), ("a", 3), ("a", 4)
            ), 2
        )

        val sortRDD: RDD[(String, Int)] = rdd.sortBy(_._2)
        sortRDD.collect().foreach(println)



        // 关闭spark
        sc.stop()
    }
}
