package edu.lzu.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark23_Transform_Cogroup {
    def main(args: Array[String]): Unit = {

        // 连接spark
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("transform")
        val sc = new SparkContext(sparkConf)


        // TODO 算子 - Cogroup
        val rdd1: RDD[(String, Int)] = sc.makeRDD(
            List(
                ("a", 1), ("b", 2), ("c", 3), ("a", 4)
            )
        )

        val rdd2: RDD[(String, Int)] = sc.makeRDD(
            List(
                ("a", 5), ("c", 7), ("b", 8)
            )
        )

        // cogroup : connect + group
        // 同一个RDD中，先分组，再和另外一个RDD连接
        // val cogroupRDD: RDD[(String, (Iterable[Int], Iterable[Int]))] = rdd1.cogroup(rdd2)
        val cogroupRDD: RDD[(String, (Iterable[Int], Iterable[Int], Iterable[Int]))] = rdd1.cogroup(rdd1, rdd2)

        cogroupRDD.collect().foreach(println)



        // 关闭spark
        sc.stop()
    }
}
