package edu.lzu.spark.core.rdd.dep.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_Action_Reduce {
    def main(args: Array[String]): Unit = {

        // 连接spark
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("transform")
        val sc = new SparkContext(sparkConf)

        // TODO 算子 - Reduce
        // 聚集 RDD 中的所有元素，先聚合分区内数据，再聚合分区间数据
        val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

        // 两两聚合
        println(rdd.reduce(_ + _))


        // 关闭spark
        sc.stop()

    }
}
