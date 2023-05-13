package edu.lzu.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark22_Transform_LeftOuterJoin {
    def main(args: Array[String]): Unit = {

        // 连接spark
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("transform")
        val sc = new SparkContext(sparkConf)


        // TODO 算子 - LeftOuterJoin
        val rdd1: RDD[(String, Int)] = sc.makeRDD(
            List(
                ("a", 1), ("b", 2), ("c", 3), ("d", 4)
            )
        )

        val rdd2: RDD[(String, Int)] = sc.makeRDD(
            List(
                ("a", 5), ("c", 7), ("b", 8)
            )
        )

        // leftOuterJoin :
        //
        //
        // rightOuterJoin :
        //
        //
        val leftRDD: RDD[(String, (Int, Option[Int]))] = rdd1.leftOuterJoin(rdd2)
        val rightRDD: RDD[(String, (Option[Int], Int))] = rdd1.rightOuterJoin(rdd2)
        leftRDD.collect().foreach(println)

        rightRDD.collect().foreach(println)

        

        // 关闭spark
        sc.stop()
    }
}
