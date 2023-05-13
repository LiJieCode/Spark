package edu.lzu.spark.core.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark04_RDD_File_Par2 {
    def main(args: Array[String]): Unit = {

        // 连接spark
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("rdd1")
        val sc = new SparkContext(sparkConf)

        // 创建RDD
        // 从文件中创建RDD,将文件中的数据作为处理的数据源  -  textFile

        // TODO 计算分区数量
        // word.txt 总字节为14
        // 14 / 2 = 7(byte)  整除，所以共有两个分区

        // TODO 计算分区存放位置
        /*
            1234567@@ => 0 1 2 3 4 5 6 7 8
            89@@      => 9 10 11 12
            0         => 13

            0分区 =》1234567@@
            1分区 =》89@@0

         */

        // 如果读取多个文件，以文件为单位，依次分区。
        val rdd: RDD[String] = sc.textFile("data/word.txt", 2)

        rdd.saveAsTextFile("output")



        // 关闭连接
        sc.stop()


    }
}
