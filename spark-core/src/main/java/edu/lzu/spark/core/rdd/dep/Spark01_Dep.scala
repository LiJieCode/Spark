package edu.lzu.spark.core.rdd.dep

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_Dep {
    def main(args: Array[String]): Unit = {


        val sc: SparkContext = new SparkContext(new SparkConf().setMaster("local[*]").setAppName("WC"))


        // toDebugString 显示血缘关系

        val rdd: RDD[String] = sc.textFile("data/word.txt")
        println(rdd.toDebugString)
        println("----------------------------------------------------")


        val words: RDD[String] = rdd.flatMap(_.split(" "))
        println(words.toDebugString)
        println("----------------------------------------------------")


        val wordToOne: RDD[(String, Int)] = words.map((_, 1))
        println(wordToOne.toDebugString)
        println("----------------------------------------------------")


        val wc: RDD[(String, Int)] = wordToOne.reduceByKey(_ + _)
        println(wc.toDebugString)
        println("----------------------------------------------------")


        wc.collect().foreach(println)



    }

}
