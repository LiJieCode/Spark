package edu.lzu.spark.core.rdd.dep.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark10_Action_Foreach {
    def main(args: Array[String]): Unit = {

        // 连接spark
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("transform")
        val sc = new SparkContext(sparkConf)

        // TODO 算子 - Foreach
        //
        val rdd: RDD[Int] = sc.makeRDD(List(
            1, 2, 3, 4
        ))

        // foreach 其实是Driver端内存集合的循环遍历方法
        rdd.collect().foreach(println)

        println(("*****************************"))    // 在Driver端执行

        // foreach 其实是Executor端内存数据打印（分布式打印，所以乱序）
        rdd.foreach(println)

        // 算子：Operator（操作）
        //      RDD 的方法和Scala集合对象的方法不一样
        //      集合对象的方法都是在同一个节点的内存中完成的
        //      RDD 的方法可以将计算逻辑发送到Executor端（分布式节点）执行
        //      为了区分不的处理效果，所以将RDD的方法称之为算子
        //      RDD的方法外部的操作都是在Driver端执行的，而方法内部的逻辑代码都是在Executor


        // 关闭spark
        sc.stop()
    }
}
