package edu.lzu.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark17_Transform_AggregateByKey {
    def main(args: Array[String]): Unit = {

        // 连接spark
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("transform")
        val sc = new SparkContext(sparkConf)


        // TODO 算子 - AggregateByKey
        val rdd: RDD[(String, Int)] = sc.makeRDD(
            List(
                ("a", 1), ("a", 2), ("b", 3), ("a", 4),("a", 3), ("b", 4)
            ), 2
        )

        // aggregateByKey 相同key的数据，进行value数据的操作
        // aggregateByKey存在函数柯里化，有两个参数列表
        // 第一个参数列表，传递一个参数，初始值
        //      主要用于当碰到第一个key时，和value进行分区内计算
        // 第二个参数列表，需要传入两个参数
        //      第一个参数表示分区内的计算规则
        //      第二个参数表示分区间的计算规则
        val aggregateRDD: RDD[(String, Int)] = rdd.aggregateByKey(0)(
            (x, y) => math.max(x, y),
            (x, y) => x + y
        )

        aggregateRDD.collect().foreach(println)


        // 分区内和分区间的计算方式一样
        val aggregateRDD1: RDD[(String, Int)] = rdd.aggregateByKey(0)(
            (x, y) => x + y,
            (x, y) => x + y
        )
        // 这样写，比较麻烦，spark提供了另外一个函数foldByKey



        // 关闭spark
        sc.stop()
    }
}
