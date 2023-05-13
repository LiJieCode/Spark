package edu.lzu.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark05_Transform_Test {
    def main(args: Array[String]): Unit = {

        // 连接spark
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("transform")
        val sc = new SparkContext(sparkConf)


        // TODO 算子 - Glom
        val rdd: RDD[Int] = sc.makeRDD(
            List(1,2,3,4), 2
        )

        val glomRDD: RDD[Array[Int]] = rdd.glom()

        // 这里就是把每一个分区的数据 作为一个数组
        val mapRDD: RDD[Int] = glomRDD.map(
            array => {
                array.max
            }
        )

        println(mapRDD.collect().sum)



        // 关闭spark
        sc.stop()

    }
}
