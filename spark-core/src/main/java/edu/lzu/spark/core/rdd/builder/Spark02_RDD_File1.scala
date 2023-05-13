package edu.lzu.spark.core.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_File1 {
    def main(args: Array[String]): Unit = {

        // 连接spark
        val sparkConf = new SparkConf().setMaster("local").setAppName("rdd1")
        val sc = new SparkContext(sparkConf)

        // 创建RDD
        // 从文件中创建RDD,将文件中的数据作为处理的数据源
        // path路径默认以当前环境的根目录作为基准。可以写绝对路径，也可以写相对路径

        // textFile ：以行为单位读取数据，不关心数据来自哪个文件
        // wholeTextFiles ：以文件为单位读取数据。读取结果是一个元组（文件地址，文件内容）
        val rdd: RDD[(String, String)] = sc.wholeTextFiles("data")
        rdd.collect().foreach(println)

        // 关闭连接
        sc.stop()


    }
}
