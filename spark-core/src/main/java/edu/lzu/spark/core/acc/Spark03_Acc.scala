package edu.lzu.spark.core.acc

import org.apache.spark.rdd.RDD
import org.apache.spark.util.LongAccumulator
import org.apache.spark.{SparkConf, SparkContext}

object Spark03_Acc {
    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Acc")
        val sc = new SparkContext(sparkConf)

        val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

        // 获取系统的累加器
        // Spark 默认提供了简单数据聚合的累加器
        val sumAcc: LongAccumulator = sc.longAccumulator("sum")
        // sc.doubleAccumulator()
        // sc.collectionAccumulator()


        val mapRDD: RDD[Int] = rdd.map(
            num => {
                // 使用累加器
                sumAcc.add(num)
                num
            }
        )
        // 少加：转换算子中调用累加器，如果没有调用行动算子，就不会执行

        mapRDD.collect()
        mapRDD.collect()
        // 多加：转换算子中调用累加器，如果多次调用行动算子，就会多加

        // 一般情况下，累加器会放在行动算子中操作
        // 获取累加器的值
        println(sumAcc.value)


        sc.stop()
    }

}
