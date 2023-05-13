package edu.lzu.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object Spark15_Transform_ReduceByKey {
    def main(args: Array[String]): Unit = {

        // 连接spark
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("transform")
        val sc = new SparkContext(sparkConf)


        // TODO 算子 - ReduceByKey
        val rdd: RDD[(String, Int)] = sc.makeRDD(
            List(
                ("a", 1), ("a", 2), ("b", 3), ("b", 4), ("c", 5)
            )
        )

        // reduceByKey : 相同key的数据，进行value数据的聚合操作
        // scala语言中，一般的聚合操作都是两两聚合，spark基于scala开发的，所以它的聚合也是两两聚合
        // 【1，2，3】
        // 【3，3】
        // 【6】
        // scala 中的reduceByKey更为复杂，复习
        // reduceByKey 中如果key的数据只有一个，是不会参与运算的。
        val reduceRDD: RDD[(String, Int)] = rdd.reduceByKey(
            (x, y) => x + y
        )
        reduceRDD.collect().foreach(println)


        // 关闭spark
        sc.stop()
    }
}
