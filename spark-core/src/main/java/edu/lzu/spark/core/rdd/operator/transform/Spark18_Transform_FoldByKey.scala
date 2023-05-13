package edu.lzu.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark18_Transform_FoldByKey {
    def main(args: Array[String]): Unit = {

        // 连接spark
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("transform")
        val sc = new SparkContext(sparkConf)


        // TODO 算子 - FoldByKey
        val rdd: RDD[(String, Int)] = sc.makeRDD(
            List(
                ("a", 1), ("a", 2), ("a", 3), ("a", 4)
            ), 2
        )

        //
        val foldRDD: RDD[(String, Int)] = rdd.foldByKey(0)(_ + _)
        val reduceRDD: RDD[(String, Int)] = rdd.reduceByKey(_ + _)
        foldRDD.collect().foreach(println)
        reduceRDD.collect().foreach(println)



        // 关闭spark
        sc.stop()
    }
}
