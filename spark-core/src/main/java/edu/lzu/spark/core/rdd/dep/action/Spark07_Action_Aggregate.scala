package edu.lzu.spark.core.rdd.dep.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark07_Action_Aggregate {
    def main(args: Array[String]): Unit = {

        // 连接spark
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("transform")
        val sc = new SparkContext(sparkConf)

        // TODO 算子 - Aggregate
        // 分区的数据通过初始值和分区内的数据进行聚合，然后再和初始值进行分区间的数据聚合
        val rdd: RDD[Int] = sc.makeRDD(List(1,3,8, 2, 7, 4))


        // aggregate
        // aggregate 存在函数柯里化，有两个参数列表
        // 第一个参数列表，传递一个参数，初始值
        //      主要用于当碰到第一个数据时，和value进行分区内计算
        // 第二个参数列表，需要传入两个参数
        //      第一个参数表示分区内的计算规则
        //      第二个参数表示分区间的计算规则
        val i: Int = rdd.aggregate(0)(_ + _, _ + _)
        println(i)


        // aggregate  和  aggregateByKey  的区别 ？？？？？？？？？？？？？ 重要 ？？？？？？？？？？？？？？？？？？？？？？？？
        // aggregateByKey ： 初始值，只会参与分区内的运算
        // aggregate ： 初始值，既参与分区内的计算，也参与分区间的计算


        // fold 折叠操作， aggregate 的简化版操作
        // 当分区内和分区间的运算规则一样是，可以用fold
        val j: Int = rdd.fold(0)(_ + _)
        println(j)


        // 关闭spark
        sc.stop()

    }
}
