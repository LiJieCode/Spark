package edu.lzu.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark13_Transform_DoubleValue {
    def main(args: Array[String]): Unit = {

        // 连接spark
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("transform")
        val sc = new SparkContext(sparkConf)


        // TODO 算子 - DoubleValue
        // - intersection
        // - union
        // - subtract
        // - zip
        val rdd1: RDD[Int] = sc.makeRDD(
            List(1, 2, 3, 4)
        )

        val rdd2: RDD[Int] = sc.makeRDD(
            List(2, 4, 5, 6)
        )

        // **********交集，并集，差集，要求两个RDD的数据类型一致，否则编译不通过

        // 交集intersection
        val rdd3: RDD[Int] = rdd1.intersection(rdd2)
        println(rdd3.collect().mkString(","))


        // 并集union
        val rdd4: RDD[Int] = rdd1.union(rdd2)
        println(rdd4.collect().mkString(","))


        // 差集subtract
        val rdd5: RDD[Int] = rdd1.subtract(rdd2)
        println(rdd5.collect().mkString(","))

        // 拉链zip
        // *******两个RDD数据数量不同，不能拉链，会报错（但是在Scala中是可以拉的）
        // *******两个RDD分区不相等，不能拉链，会报错
        val rdd6: RDD[(Int, Int)] = rdd1.zip(rdd2)
        println(rdd6.collect().mkString(","))


        // 关闭spark
        sc.stop()

    }
}
