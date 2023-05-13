package edu.lzu.spark.core.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_RDD_File {
    def main(args: Array[String]): Unit = {

        // 连接spark
        val sparkConf = new SparkConf().setMaster("local").setAppName("rdd1")
        val sc = new SparkContext(sparkConf)

        // 创建RDD
        // 从文件中创建RDD,将文件中的数据作为处理的数据源
        // path路径默认以当前环境的根目录作为基准。可以写绝对路径，也可以写相对路径
        val rdd: RDD[String] = sc.textFile("data/1.txt")
        rdd.collect().foreach(println)

        println("=================================")

        // path也可以指向某个目录
        val rdd1: RDD[String] = sc.textFile("data")
        rdd1.collect().foreach(println)

        println("=================================")

        // path中也可以使用通配符
        val rdd2: RDD[String] = sc.textFile("data/*.txt")
        rdd2.collect().foreach(println)

        println("=================================")

        // path也可以是分布式存储路径：HDFS
        // val rdd3: RDD[String] = sc.textFile("hdfs://l9z102:8020/...")
        // rdd3.collect().foreach(println)

        // 关闭连接
        sc.stop()


    }
}
