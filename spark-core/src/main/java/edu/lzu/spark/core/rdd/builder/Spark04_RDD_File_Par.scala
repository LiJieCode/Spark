package edu.lzu.spark.core.rdd.builder

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark04_RDD_File_Par {
    def main(args: Array[String]): Unit = {

        // 连接spark
        // Local模式就是运行在一台计算机上的模式，通常就是用于在本机上练手和测试。它可以通过以下集中方式设置Master
        // local: 所有计算都运行在一个线程当中，没有任何并行计算，通常我们在本机执行一些测试代码，或者练手，就用这种模式;
        // local[K]: 指定使用几个线程来运行计算，比如local[4]就是运行4个Worker线程。通常我们的Cpu有几个Core，就指定几个线程，最大化利用Cpu的计算能力;
        // local[*]: 这种模式直接帮你按照Cpu最多Cores来设置线程数了。

        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("rdd1")
        val sc = new SparkContext(sparkConf)

        // 创建RDD
        // 从文件中创建RDD,将文件中的数据作为处理的数据源  -  textFile
        // path路径默认以当前环境的根目录作为基准。可以写绝对路径，也可以写相对路径

        // textFile() 默认也可以设定分区，
        // minPartitions ： 最小分区数量
        // math.min(defaultParallelism, 2)
        // defaultParallelism 默认是计算机的核数(线程数)，见 Spark03_RDD_Memory_Par

        // 如果不想使用默认的分区数量，可以通过 textFile 第二个参数指定分区
        // 默认最小的2, 现在修改为3
        val rdd: RDD[String] = sc.textFile("data/1.txt", 3)

        rdd.saveAsTextFile("output")



        // 关闭连接
        sc.stop()


    }
}
