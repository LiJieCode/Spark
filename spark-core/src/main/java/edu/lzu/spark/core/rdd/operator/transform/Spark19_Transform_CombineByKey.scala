package edu.lzu.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark19_Transform_CombineByKey {
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

        // combineByKey 需要三个参数
        // 第一个参数，将相同key的第一个数据进行结构转换
        // 第二个参数，分区内的计算规则
        // 第三个参数，分区间的计算规则
        val comRDD: RDD[(String, (Int, Int))] = rdd.combineByKey(
            v => (v, 1), (t1: (Int, Int), v) => (t1._1 + v, t1._2 + 1), (t1: (Int, Int), t2: (Int, Int)) => (t1._1 + t2._1, t1._2 + t2._2)
        )

        val mapRDD: RDD[(String, Double)] = comRDD.mapValues {
            case (num, cnt) => num.toDouble / cnt

        }

        mapRDD.collect().foreach(println)




        // 关闭spark
        sc.stop()
    }
}
