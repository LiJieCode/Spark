package edu.lzu.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark24_Transform_Exercise {
    def main(args: Array[String]): Unit = {

        // 连接spark
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("transform")
        val sc = new SparkContext(sparkConf)


        // TODO 案例实操
        // agent.log：时间戳，省份，城市，用户，广告，中间字段使用空格分隔。
        // 需求描述 ： 统计出每一个省份每个广告被点击数量排行的 Top3

        // 获取原始数据 ： 时间戳，省份，城市，用户，广告
        val rdd: RDD[String] = sc.textFile("data/agent.log")

        // 将原始数据进行结构转换 ： ((省份, 广告), 1)
        val mapRDD: RDD[((String, String), Int)] = rdd.map(
            line => {
                val data: Array[String] = line.split(" ")
                ((data(1), data(4)), 1)
            }
        )

        val reduceRDD: RDD[((String, String), Int)] = mapRDD.reduceByKey(_ + _)

        val mapRDD1: RDD[(String, (String, Int))] = reduceRDD.map(
            t1 => {
                (t1._1._1, (t1._1._2, t1._2))
            }
        )

        val groupRDD: RDD[(String, Iterable[(String, Int)])] = mapRDD1.groupByKey()

        //  组内排序
        // 当操作和key无关时，就用mapValues
        val mapRDD2: RDD[(String, List[(String, Int)])] = groupRDD.mapValues(
            iter => {
                iter.toList.sortBy(_._2)(Ordering.Int.reverse).take(3) // ?????????????????????????????重要
            }
        )




        mapRDD2.collect().foreach(println)





        // 关闭spark
        sc.stop()
    }
}
