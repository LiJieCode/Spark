package edu.lzu.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark06_Transform_Test1 {
    def main(args: Array[String]): Unit = {

        // 连接spark
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("transform")
        val sc = new SparkContext(sparkConf)


        // TODO 算子 - GroupBy
        val rdd: RDD[String] = sc.makeRDD(
            List("java", "spark", "scala", "hadoop"), 2
        )

        // 分组和分区没有必然的联系
        // 分组和分区 底层是有逻辑的
        // groupBy会将数据打乱，重新组合，这个操作我们称之为shuffle
        val groupRDD: RDD[(Char, Iterable[String])] = rdd.groupBy(
            s => {
                s.charAt(0)
            }
        )

        groupRDD.collect().foreach(println)
        // groupRDD.saveAsTextFile("output")


        // 关闭spark
        sc.stop()

    }
}
