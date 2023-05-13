package edu.lzu.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark12_Transform_Test {
    def main(args: Array[String]): Unit = {

        // 连接spark
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("transform")
        val sc = new SparkContext(sparkConf)


        // TODO 算子 - SortBy
        val rdd: RDD[(String, Int)] = sc.makeRDD(
            List(("1", 1), ("11", 5), ("2", 3))
        )


        val newRDD: RDD[(String, Int)] = rdd.sortBy(_._2.toInt, false)

        newRDD.collect().foreach(println)




        // 关闭spark
        sc.stop()

    }
}
