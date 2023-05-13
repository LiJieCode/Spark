package edu.lzu.spark.core.bc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_Bc {
    def main(args: Array[String]): Unit = {

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Bc")
        val sc = new SparkContext(sparkConf)

        val rdd1: RDD[(String, Int)] = sc.makeRDD(List(
            ("a", 1), ("b", 2), ("c", 3)
        ))

        val rdd2: RDD[(String, Int)] = sc.makeRDD(List(
            ("a", 4), ("b", 5), ("c", 6)
        ))

        // join 会导致数据量的集合增长，会影响shuffle的性能，不推荐使用
        // val joinRDD: RDD[(String, (Int, Int))] = rdd1.join(rdd2)
        // joinRDD.collect().foreach(println)





        sc.stop()


    }

}
