package edu.lzu.spark.core.rdd.io

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_Save {
    def main(args: Array[String]): Unit = {

        val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("save"))

        val rdd: RDD[(String, Int)] = sc.makeRDD(List(
            ("a", 1),
            ("a", 2),
            ("a", 3),
            ("b", 3),
            ("c", 3)
        ))

        rdd.saveAsTextFile("output")
        rdd.saveAsObjectFile("output1")
        rdd.saveAsSequenceFile("output2")


    }

}
