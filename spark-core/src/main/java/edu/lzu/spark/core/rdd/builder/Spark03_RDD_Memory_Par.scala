package edu.lzu.spark.core.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark03_RDD_Memory_Par {
    def main(args: Array[String]): Unit = {

        // 连接spark
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("rdd1")
        // 将默认分区数修改为 5
        sparkConf.set("spark.default.parallelism", "5")
        val sc = new SparkContext(sparkConf)

        // 创建RDD
        // 从内存中创建RDD,将内存中集合的数据作为处理的数据源
        // RDD 并行度和分区
        // numSlices 如果不给出，会采用默认值：defaultParallelism
        // 源码：scheduler.conf.getInt("spark.default.parallelism", totalCores)
        // totalCores (运行环境的核数)，这里采用本地，所以是计算机的核数
        // spark.default.parallelism 可以在配置对象中修改，见上
        val rdd = sc.makeRDD(
            List(1, 2, 3, 4),
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
