package edu.lzu.spark.core.rdd.dep.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark06_Action_TakeOrdered {
    def main(args: Array[String]): Unit = {

        // 连接spark
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("transform")
        val sc = new SparkContext(sparkConf)

        // TODO 算子 - TakeOrdered
        // 返回该 RDD 排序后的前 n 个元素组成的数组
        val rdd: RDD[Int] = sc.makeRDD(List(1,3,8, 2, 7, 4))

        //
        val ints: Array[Int] = rdd.takeOrdered(3)(Ordering.Int.reverse)   // 降序

        ints.foreach(println)

        // 关闭spark
        sc.stop()

    }
}
