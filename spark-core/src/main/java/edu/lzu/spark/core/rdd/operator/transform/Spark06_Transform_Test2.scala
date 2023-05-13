package edu.lzu.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import java.text.SimpleDateFormat
import java.util.Date

object Spark06_Transform_Test2 {
    def main(args: Array[String]): Unit = {

        // 连接spark
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("transform")
        val sc = new SparkContext(sparkConf)


        // TODO 算子 - GroupBy
        val rdd: RDD[String] = sc.textFile("data/apache.log")

        // 19/05/2015:15:05:08
        // 小功能： 从服务器日志数据 apache.log 中获取每个时间段访问量。
        val mapRDD: RDD[(String, Int)] = rdd.map(
            line => {
                val data: Array[String] = line.split(" ")
                val time: String = data(3)
                val sdf = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
                val date: Date = sdf.parse(time)
                val sdf1 = new SimpleDateFormat("HH")
                val hour: String = sdf1.format(date)
                (hour, 1)
            }
        )

        //        val groupRDD: RDD[(String, Iterable[(String, Int)])] = mapRDD.groupBy{
        //            case (hour ,1 ) => {
        //                hour
        //            }
        //        }

        val groupRDD: RDD[(String, Iterable[(String, Int)])] = mapRDD.groupBy(_._1)

        val mapGroupRDD: RDD[(String, Int)] = groupRDD.map(
            iter => {
                (iter._1, iter._2.size)
            }
        )

        mapGroupRDD.collect().foreach(println)

        // 关闭spark
        sc.stop()

    }
}
