package edu.lzu.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark06_Transform_Test {
    def main(args: Array[String]): Unit = {

        // 连接spark
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("transform")
        val sc = new SparkContext(sparkConf)


        // TODO 算子 - GroupBy
        val rdd: RDD[String] = sc.makeRDD(
            List("hello", "spark", "scala", "hadoop"), 2
        )

        // 需求：对List("hello", "spark", "scala", "hadoop")，将首字母相同的单词放在一个组内
        val groupRDD: RDD[(Char, Iterable[String])] = rdd.groupBy(
            s => {
                s.charAt(0)
            }
        )

        groupRDD.collect().foreach(println)



        // 关闭spark
        sc.stop()

    }
}
