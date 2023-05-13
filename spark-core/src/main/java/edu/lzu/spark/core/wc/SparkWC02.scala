package edu.lzu.spark.core.wc

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object SparkWC02 {
  def main(args: Array[String]): Unit = {

    // TODO 建立连接
    val sparkConf = new SparkConf().setMaster("local").setAppName("WorkCount")
    val sc = new SparkContext(sparkConf)

    // 11 种 workCount  -  P85-86




    // TODO 关闭连接
    sc.stop()
  }

}
