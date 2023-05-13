package edu.lzu.spark.core.rdd.dep.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark05_Action_Take {
    def main(args: Array[String]): Unit = {

        // 连接spark
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("transform")
        val sc = new SparkContext(sparkConf)

        // TODO 算子 - Take
        // 返回一个由 RDD 的前 n 个元素组成的数组
        val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

        //
        val ints: Array[Int] = rdd.take(3)


        // 关闭spark
        sc.stop()

    }
}
