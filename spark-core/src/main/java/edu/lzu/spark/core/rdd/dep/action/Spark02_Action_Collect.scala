package edu.lzu.spark.core.rdd.dep.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_Action_Collect {
    def main(args: Array[String]): Unit = {

        // 连接spark
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("transform")
        val sc = new SparkContext(sparkConf)

        // TODO 算子 - Collect
        // 在驱动程序中，以数组 Array 的形式返回数据集的所有元素
        val rdd: RDD[Int] = sc.makeRDD(List(1, 2, 3, 4))

        // collect 方法会将不同分区的数据按照分区顺序采集到Driver端内存中，形成数组
        val ints: Array[Int] = rdd.collect()


        // 关闭spark
        sc.stop()

    }
}
