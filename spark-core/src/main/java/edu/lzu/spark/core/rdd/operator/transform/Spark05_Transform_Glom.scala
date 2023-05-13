package edu.lzu.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark05_Transform_Glom {
    def main(args: Array[String]): Unit = {

        // 连接spark
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("transform")
        val sc = new SparkContext(sparkConf)


        // TODO 算子 - Glom
        val rdd: RDD[Int] = sc.makeRDD(
            List(1,2,3,4), 2
        )

        // flatMap  list => int
        // glom     int  => array

        val glomRDD: RDD[Array[Int]] = rdd.glom()

        // println(glomRDD.collect().mkString(","))
        glomRDD.collect().foreach(
            data => println(data.mkString(","))
        )


        // 关闭spark
        sc.stop()

    }
}
