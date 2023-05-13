package edu.lzu.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import java.text.SimpleDateFormat
import java.util.Date

object Spark07_Transform_Test {
    def main(args: Array[String]): Unit = {

        // 连接spark
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("transform")
        val sc = new SparkContext(sparkConf)


        // TODO 算子 - Filter
        // 从服务器日志数据 apache.log 中获取 2015 年 5 月 17 日的请求路径
        val rdd: RDD[String] = sc.textFile("data/apache.log")

        val mapRDD = rdd.map(
            line => {
                val data: Array[String] = line.split(" ")
                val timeString = data(3)
                val sdf = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
                val date: Date = sdf.parse(timeString)
                val sdf1 = new SimpleDateFormat("dd/MM/yyyy")
                val time: String = sdf1.format(date)
                (time, data(6))
            }
        )

        val filterRDD: RDD[(String, String)] = mapRDD.filter(_._1.equals("17/05/2015"))

        filterRDD.collect().foreach(println)


        // 关闭spark
        sc.stop()

    }
}
