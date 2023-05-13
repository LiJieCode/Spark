package edu.lzu.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_Transform_Test {
    def main(args: Array[String]): Unit = {

        // 连接spark
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("transform")
        val sc = new SparkContext(sparkConf)


        // TODO 算子 - Map 案例实操
        val rdd1: RDD[String] = sc.textFile("data/apache.log")

        // 小功能：从服务器日志数据 apache.log 中获取用户请求 URL 资源路径
        val mapRDD1: RDD[String] = rdd1.map(
            line => {
                val data = line.split(" ")
                data(6)
            }
        )

        mapRDD1.collect().foreach(println)


        // 关闭spark
        sc.stop()

    }
}
