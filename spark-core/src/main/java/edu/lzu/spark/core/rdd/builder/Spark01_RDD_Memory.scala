package edu.lzu.spark.core.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_RDD_Memory {
    def main(args: Array[String]): Unit = {

        // 连接spark
        val sparkConf = new SparkConf().setMaster("local").setAppName("rdd1")
        val sc = new SparkContext(sparkConf)

        // 创建RDD
        // 从内存中创建RDD,将内存中集合的数据作为处理的数据源
        val rdd1: RDD[Int] = sc.parallelize(List(1, 2, 3, 4))
        val rdd2: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

        rdd1.collect().foreach(println)

        // 关闭连接
        sc.stop()


    }
}
