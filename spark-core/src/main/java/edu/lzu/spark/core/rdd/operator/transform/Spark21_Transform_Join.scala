package edu.lzu.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark21_Transform_Join {
    def main(args: Array[String]): Unit = {

        // 连接spark
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("transform")
        val sc = new SparkContext(sparkConf)


        // TODO 算子 - Join
        val rdd1: RDD[(String, Int)] = sc.makeRDD(
            List(
                ("a", 1), ("a", 2), ("c", 3), ("d", 4)
            )
        )

        val rdd2: RDD[(String, Int)] = sc.makeRDD(
            List(
                ("a", 5), ("c", 7), ("a", 8)
            )
        )

        // join : 两个不同数据源的数据，相同的key的value会连接在一起，形成元组
        //        如果两个数据源中的key没有匹配上，那么数据不会出现在结果中
        //        如果相同的key有多个，会依次相互匹配，会出现笛卡尔乘积，数据量会几何性增长
        val joinRDD: RDD[(String, (Int, Int))] = rdd1.join(rdd2)

        joinRDD.collect().foreach(println)



        // 关闭spark
        sc.stop()
    }
}
