package edu.lzu.spark.core.rdd.persist

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark03_Persist {
    def main(args: Array[String]): Unit = {

        // 连接spark
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("transform")
        val sc = new SparkContext(sparkConf)
        // 设置检查点保存路径，一般是保存到分布式存储系统中：HDFS
        sc.setCheckpointDir("cp")


        // TODO 算子 - Persist
        // cache persist  checkPoint 区别
        /**
         * cache ： 将数据临时存储在内存中进行数据重用
         * persist ： 将数据临时存储在磁盘文件中进行数据重用
         *            涉及到磁盘IO，性能较低，但是数据安全
         *            如果作业执行完毕，临时保存的数据文件就会丢失
         * checkPoint ：将数据长久地保存在磁盘文件中，进行数据重用  （不仅仅是临时，可以跨作业使用）
         *              涉及到磁盘IO，性能较低，但是数据安全
         *              为了保证数据安全，所以一般情况下，会独立执行作业之后再保存，不是将已经存在的RDD数据保存，而是再重新执行一遍之后保存
         *              为了能够执行效率，一般是和cache联合使用
         */


        val rdd: RDD[String] = sc.makeRDD(List(
            "hello spark", "hello java", "java scala"
        ))

        val flatRDD: RDD[String] = rdd.flatMap(_.split(" "))

        val mapRDD: RDD[(String, Int)] = flatRDD.map(
            s => {
                println("######################")
                (s, 1)
            }
        )

        // checkpoint 和 cache 联合使用，避免checkpoint再次独立执行一遍作业
        mapRDD.cache()
        mapRDD.checkpoint()  // 这样就避免了checkpoint再次独立执行一遍作业

        val reduceRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_ + _)

        reduceRDD.collect().foreach(println)

        println("=================================================")

        val groupRDD: RDD[(String, Iterable[Int])] = mapRDD.groupByKey()

        groupRDD.collect().foreach(println)



        // 关闭spark
        sc.stop()
    }


}
