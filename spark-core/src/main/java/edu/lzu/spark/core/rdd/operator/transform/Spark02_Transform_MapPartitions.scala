package edu.lzu.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_Transform_MapPartitions {
    def main(args: Array[String]): Unit = {

        // 连接spark
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("transform")
        val sc = new SparkContext(sparkConf)

        // TODO 算子 - MapPartition
        val rdd1: RDD[Int] = sc.makeRDD(
            List(1, 2, 3, 4), 2
        )

        // mapPartitions:
        // 可以以分区为单位进行数据转换操作，但是会将整个分区的引用数据加载到内存进行引用
        // 因此处理完的数据是不会释放的，存在对象的引用
        // 在内存较小，数据量较大时，容易出现内存溢出
        // map 则是来一条处理一条数据，然后释放？？？？？？？？？？？？？？？？？？？？？？？？？？？？？？？？？？？
        val mapRDD = rdd1.mapPartitions(
            iter => {
                println(">>>>>>>>>>>>>>>")
                iter.map(_ * 2)
            }
        )
        mapRDD.collect().foreach(println)

        // 关闭spark
        sc.stop()

    }
}
