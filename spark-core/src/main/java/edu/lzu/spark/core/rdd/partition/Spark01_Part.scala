package edu.lzu.spark.core.rdd.partition

import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, SparkConf, SparkContext}

/**
 *
 * 自定义分区规则
 *
 */
object Spark01_Part {
    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("part")

        val sc = new SparkContext(sparkConf)


        val rdd: RDD[(String, Int)] = sc.makeRDD(List(
            ("nba", 12),
            ("cba", 12),
            ("wnba", 12),
            ("nba", 13),
            ("cba", 13)
        ), 3)

        val rdd1: RDD[(String, Int)] = rdd.partitionBy(new MyPartitioner)


        rdd1.saveAsTextFile("output")



    }

    // 自定义分区规则
    class MyPartitioner extends Partitioner{

        // 分区数量
        override def numPartitions: Int = 3

        // 返回数据的分区号
        // 根据数据的key值，返回数据所在的分区索引（从0开始）
        override def getPartition(key: Any): Int = {
           key match {
               case "nba" => 0
               case "cba" => 1
               case "wnba" => 2
               case _ => 3
           }

//            if (key == "nba") 0
//            else if (key == "cba") 1
//            else if (key == "wnba") 2
//            else 3
        }
    }

}
