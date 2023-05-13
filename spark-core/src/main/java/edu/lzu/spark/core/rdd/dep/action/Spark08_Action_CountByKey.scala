package edu.lzu.spark.core.rdd.dep.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark08_Action_CountByKey {
    def main(args: Array[String]): Unit = {

        // 连接spark
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("transform")
        val sc = new SparkContext(sparkConf)

        // TODO 算子 - CountByKey  CountByValue
        // 统计每种 key 的个数
        val rdd: RDD[(String, Int)] = sc.makeRDD(List(
            ("a", 11), ("a", 12), ("b", 13)
        ))

        val stringToLong: collection.Map[String, Long] = rdd.countByKey()

        stringToLong.foreach(println)

        // countByValue 统计数据出现的次数
        val rdd1: RDD[Int] = sc.makeRDD(List(
            1, 1, 2, 2, 2, 3, 3, 3, 4, 4, 4, 4
        ))

        val intToLong: collection.Map[Int, Long] = rdd1.countByValue()

        intToLong.foreach(println)

        // 关闭spark
        sc.stop()

    }
}
