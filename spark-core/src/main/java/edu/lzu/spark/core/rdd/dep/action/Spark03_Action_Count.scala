package edu.lzu.spark.core.rdd.dep.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark03_Action_Count {
    def main(args: Array[String]): Unit = {

        // 连接spark
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("transform")
        val sc = new SparkContext(sparkConf)

        // TODO 算子 - Count
        // 返回 RDD 中元素的个数
        val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

        // 数据源中，数据的个数
        val l: Long = rdd.count()

        println(l)


        // 关闭spark
        sc.stop()

    }
}
