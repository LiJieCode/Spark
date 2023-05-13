package edu.lzu.spark.core.rdd.serial

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_Serial {
    def main(args: Array[String]): Unit = {

        // 连接spark
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("transform")
        val sc = new SparkContext(sparkConf)

        // TODO 算子 - Serial
        //
        val rdd: RDD[Int] = sc.makeRDD(List(
            1, 2, 3, 4
        ))




        // 关闭spark
        sc.stop()
    }


}
