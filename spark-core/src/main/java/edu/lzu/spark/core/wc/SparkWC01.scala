package edu.lzu.spark.core.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkWC01 {
  def main(args: Array[String]): Unit = {

    // TODO 建立连接
    val sparkConf = new SparkConf().setMaster("local").setAppName("WorkCount")
    val sc = new SparkContext(sparkConf)

    // TODO 执行业务
    val lines: RDD[String] = sc.textFile("data")

    val words: RDD[String] = lines.flatMap(_.split(" "))

    val wordGroup: RDD[(String, Iterable[String])] = words.groupBy(s => s)

    val wordToCount = wordGroup.map {
      case ( word, list ) => {
        (word, list.size)
      }
    }

    val array: Array[(String, Int)] = wordToCount.collect()
    array.foreach(println)


    // TODO 关闭连接
    sc.stop()
  }

}
