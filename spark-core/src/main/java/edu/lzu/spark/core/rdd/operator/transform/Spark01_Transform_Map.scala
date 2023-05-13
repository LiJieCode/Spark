package edu.lzu.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_Transform_Map {
    def main(args: Array[String]): Unit = {

        // 连接spark
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("transform")
        val sc = new SparkContext(sparkConf)

        // TODO 算子 - Map
        val rdd1: RDD[Int] = sc.makeRDD(
            List(1, 2, 3, 4)
        )
        // 转换函数
        //        def mapFunction(num: Int): Int ={
        //            num * 2
        //        }
        //        val mapRDD = rdd1.map(mapFunction)

        //        val mapRDD = rdd1.map((num: Int) => {num * 2})
        //        val mapRDD = rdd1.map((num: Int) => num * 2)
        //        val mapRDD = rdd1.map((num) => num * 2)
        //        val mapRDD = rdd1.map(num => num * 2)
        val mapRDD1 = rdd1.map(_ * 2)

        mapRDD1.collect().foreach(println)


        // 关闭spark
        sc.stop()

    }
}
