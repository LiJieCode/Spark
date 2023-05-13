package edu.lzu.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark17_Transform_Test {
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
        // aggRDD的数据类型，取决于传入的初始值类型
        val aggRDD: RDD[(String, String)] = rdd.aggregateByKey("")(_ + _, _ + _)

        // 获取相同key的数据的平均值
        val aggRDD1: RDD[(String, (Int, Int))] = rdd.aggregateByKey((0, 0))(
            (t, v) => {
                (t._1 + v, t._2 + 1)
            }, (x, y) => {
                (x._1 + y._1, x._2 + y._2)
            }
        )

//        val mapRDD: RDD[(String, Double)] = aggRDD1.map(
//            x => (x._1, x._2._1.toDouble  / x._2._2)
//        )

        // 输入值是元组的时候，要用模式匹配
//        val mapRDD: RDD[(String, Double)] = aggRDD1.map {
//            case (x, y) => {
//                (x, y._1.toDouble / y._2)
//            }
//        }

        // 功能实现画图 - P72 未看
        val mapRDD: RDD[(String, Double)] = aggRDD1.mapValues {
            case (num, cnt) => {
                num.toDouble / cnt
            }
        }

        mapRDD.collect().foreach(println)


        // 关闭spark
        sc.stop()
    }
}
