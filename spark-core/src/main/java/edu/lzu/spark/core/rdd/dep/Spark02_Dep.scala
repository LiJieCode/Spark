package edu.lzu.spark.core.rdd.dep

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_Dep {
    def main(args: Array[String]): Unit = {


        val sc: SparkContext = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("WC"))


        // dependencies  显示相邻的两个RDD的依赖关系

        // OneToOneDependency（窄依赖） 新RDD一个分区的数据来自上一个RDD的一个分区的数据

        // ShuffleDependency（宽依赖） 是经过shuffle的依赖关系（新RDD一个分区的数据来自上一个RDD的多个分区的数据）

        val rdd: RDD[String] = sc.textFile("data/word.txt")
        println(rdd.dependencies)
        println("----------------------------------------------------")


        val words: RDD[String] = rdd.flatMap(_.split(" "))
        println(words.dependencies)
        println("----------------------------------------------------")


        val wordToOne: RDD[(String, Int)] = words.map((_, 1))
        println(wordToOne.dependencies)
        println("----------------------------------------------------")


        val wc: RDD[(String, Int)] = wordToOne.reduceByKey(_ + _)
        println(wc.dependencies)
        println("----------------------------------------------------")


        wc.collect().foreach(println)


    }

}
