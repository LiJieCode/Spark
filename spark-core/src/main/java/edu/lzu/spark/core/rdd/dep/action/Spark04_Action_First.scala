package edu.lzu.spark.core.rdd.dep.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark04_Action_First {
    def main(args: Array[String]): Unit = {

        // 连接spark
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("transform")
        val sc = new SparkContext(sparkConf)

        // TODO 算子 - First
        // 返回 RDD 中的第一个元素
        val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

        //
        val i: Int = rdd.first()

        println(i)


        // 关闭spark
        sc.stop()

    }
}
