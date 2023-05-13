package edu.lzu.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark11_Transform_Repartition {
    def main(args: Array[String]): Unit = {

        // 连接spark
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("transform")
        val sc = new SparkContext(sparkConf)


        // TODO 算子 - Repartition
        // 学了这个之后，就不要用coalesce了，
        // 因为Repartition可以扩大,可以缩减分区，
        // 因此Repartition必须会经过shuffle处理
        val rdd: RDD[Int] = sc.makeRDD(
            List(1,2,3,4,5,6,7,8), 2
        )

        val newRDD: RDD[Int] = rdd.repartition(4)

        newRDD.saveAsTextFile("output")


        // 关闭spark
        sc.stop()

    }
}
