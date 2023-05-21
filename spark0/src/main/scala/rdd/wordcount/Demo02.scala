package rdd.wordcount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Demo02 {
    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("0516")

        val sc = new SparkContext(sparkConf)

        val rdd: RDD[String] = sc.textFile("data/1.txt")

        val wc: RDD[(String, Int)] = rdd.flatMap(
            line => {
                val datas: Array[String] = line.split(" ")
                datas.map((_, 1))
            }
        ).reduceByKey(_ + _)




//        wc.collect().foreach(println)
        wc.foreach(println)

    }
}
