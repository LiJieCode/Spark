package edu.lzu.spark.core.wc

import org.apache.spark.{SparkConf, SparkContext}

object Spark01 {
    def main(args: Array[String]): Unit = {
        // 建立和spark连接
        val sparkConf = new SparkConf().setMaster("local").setAppName("WC1")
        val sc = new SparkContext(sparkConf)

        // 业务操作

        // 关闭连接
        sc.stop()
    }

}
