package edu.lzu.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark19_Transform_Test {
    def main(args: Array[String]): Unit = {

        // 连接spark
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("transform")
        val sc = new SparkContext(sparkConf)


        // TODO 算子 - CombineByKey
        val rdd: RDD[(String, Int)] = sc.makeRDD(
            List(
                ("a", 1), ("a", 2), ("b", 3), ("a", 4),("a", 3), ("b", 4)
            ), 2
        )

        // 思考一个问题： reduceByKey、 foldByKey、 aggregateByKey、 combineByKey 的区别？
        // reduceByKey: 相同 key 的第一个数据不进行任何计算，分区内和分区间计算规则相同
        // FoldByKey: 相同 key 的第一个数据和初始值进行分区内计算，分区内和分区间计算规则相同
        // AggregateByKey：相同 key 的第一个数据和初始值进行分区内计算，分区内和分区间计算规则可以不相同
        // CombineByKey:当计算时，发现数据结构不满足要求时，可以让第一个数据转换结构。分区内和分区间计算规则不相同。

        // 视频讲解 - P74（源码分析）  未看



        // 关闭spark
        sc.stop()
    }
}
