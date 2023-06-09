package rdd.wordcount

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Demo01 {
    def main(args: Array[String]): Unit = {
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("0516")

        val sc = new SparkContext(sparkConf)

        // val wc: RDD[(String, Int)] = sc.textFile("data/1.txt").flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)

//        val wc: RDD[(String, Int)] = sc.textFile("data/1.txt").flatMap(_.split(" ")).groupBy(s => s).map {
//            case (word, list) => {
//                (word, list.size)
//            }
//        }

        // val wc: RDD[(String, Int)] = sc.textFile("").flatMap(_.split(" ")).map((_, 1)).reduceByKey(_ + _)

        val rdd: RDD[String] = sc.textFile("")

        val mapRDD: RDD[String] = rdd.flatMap(_.split(" "))

        val groupRDD: RDD[(String, Iterable[String])] = mapRDD.groupBy(s => s)

        val wc: RDD[(String, Int)] = groupRDD.map {
            case (word, list) => {
                (word, list.size)
            }
        }

        wc.foreach(println)

        sc.stop()

    }

}
