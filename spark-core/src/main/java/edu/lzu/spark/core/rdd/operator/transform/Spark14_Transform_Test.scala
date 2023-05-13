package edu.lzu.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object Spark14_Transform_Test {
    def main(args: Array[String]): Unit = {

        // 连接spark
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("transform")
        val sc = new SparkContext(sparkConf)


        // TODO 算子 - PartitionBy
        val rdd: RDD[Int] = sc.makeRDD(
            List(1, 2, 3, 4), 2
        )
        val mapRDD: RDD[(Int, Int)] = rdd.map((_, 1))

        // PartitionBy 根据指定的分区规则，对数据进行重分区

        // 可以自定义分区器，参考HashPartitioner
        // val newRDD: RDD[(Int, Int)] = mapRDD.partitionBy(new HashPartitioner(2))



        // 关闭spark
        sc.stop()

    }
}
