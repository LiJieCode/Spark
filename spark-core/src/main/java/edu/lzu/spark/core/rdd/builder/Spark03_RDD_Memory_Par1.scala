package edu.lzu.spark.core.rdd.builder

import org.apache.spark.{SparkConf, SparkContext}

object Spark03_RDD_Memory_Par1 {
    def main(args: Array[String]): Unit = {

        // 连接spark
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("rdd1")
        val sc = new SparkContext(sparkConf)

        // 创建RDD
        // 从内存中创建RDD,将内存中集合的数据作为处理的数据源
        // RDD 并行度和分区
        // 研究一下数据以什么样的方式分配到每个分区中的
        // 具体分析需要看源码，见视频P36

        val rdd = sc.makeRDD(
            List(1, 2, 3, 4)
        )

        /**
         * 总结：
         *     修改分区，可以在makeRDD()中给出分区数，也可以修改默认分区数
         */

        // saveAsTextFile 将处理的数据保存成分区文件
        rdd.saveAsTextFile("output")

        // 关闭连接
        sc.stop()
    }
}
