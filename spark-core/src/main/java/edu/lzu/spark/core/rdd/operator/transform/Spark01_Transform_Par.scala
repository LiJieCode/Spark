package edu.lzu.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_Transform_Par {
    def main(args: Array[String]): Unit = {

        // 连接spark
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("transform")
        val sc = new SparkContext(sparkConf)

        // TODO 算子 - Map 并发

        // 如果指定是一个分区，数据会依次处理，数据1两次处理完成之后，才会对数据2处理
        // 如果指定是两个分区，每个分区内数据依次处理，分区间数据处理顺序没有关系
        /*
            0分区 ：[1 2]
            1分区 ：[3 4]
            分区内有序，分区间无序
         */
        val rdd1: RDD[Int] = sc.makeRDD(
            List(1, 2, 3, 4) , 2
        )
        val mapRDD1 = rdd1.map(
            num =>{
                println(">>>>>>" + num)
                num
            }
        )
        val mapRDD2 = mapRDD1.map(
            num => {
                println("######" + num)
                num
            }
        )

        mapRDD2.collect()


        // 关闭spark
        sc.stop()

    }
}
