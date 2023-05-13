package edu.lzu.spark.core.acc

import org.apache.spark.rdd.RDD
import org.apache.spark.util.{AccumulatorV2, LongAccumulator}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable

object Spark04_Acc_Wc {
    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Acc")
        val sc = new SparkContext(sparkConf)

        val rdd: RDD[String] = sc.makeRDD(List(
            "hello", "spark", "hello"
        ))

        //累加器： 分布式共享只写变量
        // val wc1: RDD[(Int, Int)] = rdd.map((_, 1)).reduceByKey(_ + _)

        // 累加器 : 自定义累加器
        // 创建累加器对象
        val wcACC = new MyAccumulator
        // 向Spark进行注册
        sc.register(wcACC, "wordCountACC")


        rdd.foreach(
            word => {
                wcACC.add(word)
            }
        )

        println(wcACC.value)




        sc.stop()
    }

    /**
     *  自定义累加器
     *  1.继承AccumulatorV2, 定义泛型
     *    IN  : 累加器输入的数据类型
     *    OUT : 累加器返回的数据类型
     *
     *  2.重写方法  - P108  再次观看--------------------------------------
     *
     */
    class MyAccumulator extends AccumulatorV2[String, mutable.Map[String, Long]]{

        // 判断是否是初始状态
        override def isZero: Boolean = ???

        override def copy(): AccumulatorV2[String, mutable.Map[String, Long]] = ???

        override def reset(): Unit = ???

        // 获取累加器需要计算的值
        override def add(v: String): Unit = ???

        // Driver 合并多个累加器
        override def merge(other: AccumulatorV2[String, mutable.Map[String, Long]]): Unit = ???

        // 累加器结果
        override def value: mutable.Map[String, Long] = ???
    }


}
