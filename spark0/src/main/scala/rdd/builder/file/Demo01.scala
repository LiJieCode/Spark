package rdd.builder.file

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Demo01 {
    def main(args: Array[String]): Unit = {
        val sparkConf: SparkConf = new SparkConf().setMaster("local").setAppName("0513")

        val sc = new SparkContext(sparkConf)

        val rdd: RDD[String] = sc.textFile("data/1.txt")

        rdd.collect().foreach(println)

        sc.stop()
    }
}
