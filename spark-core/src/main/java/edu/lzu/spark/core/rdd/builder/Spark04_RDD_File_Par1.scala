package edu.lzu.spark.core.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark04_RDD_File_Par1 {
    def main(args: Array[String]): Unit = {

        // 连接spark
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("rdd1")
        val sc = new SparkContext(sparkConf)

        // 创建RDD
        // 从文件中创建RDD,将文件中的数据作为处理的数据源  -  textFile
        // TODO
        // 这里最小分区设置的是 2 ，但是结果却分成了3个文件， 为什么？ 看源码。
        // spark 读取文件的方式，底层采用了hadoop读取文件的方式
        // totalSize = 7  文件的总字节数
        // goalSize = 7 / 2 = 3(byte)  每个分区3个字节
        // 7 / 3 = 2 ... 1 (超过3的1.1倍) + 1 = 3(分区)

        // TODO 数据分区的分配
        // 1、spark以行为单位读取数据，
        //    spark读取文件，采用的是hadoop的方式读取，所以是一行一行读取，和字节没有关系
        // 2、数据读取时，以偏移量为单位（偏移量不会重新读取 ）
        /*
              1@@ =》 0 1 2
              2@@ =》 3 4 5
              3   =》 6
         */
         // 3.数据分区的偏移量范围的计算
         //   0分区 =》[0,3]  =>12
         //   1分区 =》[3,6]  =>3
         //   2分区 =》[6,7]  =>
         //
        val rdd: RDD[String] = sc.textFile("data/3.txt", 2)

        rdd.saveAsTextFile("output")



        // 关闭连接
        sc.stop()


    }
}
