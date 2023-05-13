package edu.lzu.spark.core.rdd.io

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


object Spark02_Load {
    def main(args: Array[String]): Unit = {

        val sc = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("save"))


        val rdd: RDD[String] = sc.textFile("output")
        println(rdd.collect().mkString(","))


        // 这里的泛型是[(String, Int)]
        val rdd1: RDD[(String, Int)] = sc.objectFile[(String, Int)]("output1")
        println(rdd1.collect().mkString(","))


        // 这里的泛型是[String, Int]
        val rdd2: RDD[(String, Int)] = sc.sequenceFile[String, Int]("output2")
        println(rdd2.collect().mkString(","))




    }

}
