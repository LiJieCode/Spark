package edu.lzu.spark.core.rdd.persist

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark02_Persist {
    def main(args: Array[String]): Unit = {

        // 连接spark
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("transform")
        val sc = new SparkContext(sparkConf)
        // 设置检查点保存路径，一般是保存到分布式存储系统中：HDFS
        sc.setCheckpointDir("cp")


        // TODO 算子 - Persist
        //
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

        // mapRDD.cache()  // 将mapRDD数据临时存到内存中
        // mapRDD.persist(StorageLevel.DISK_ONLY)   // 将mapRDD数据临时存到磁盘中，作业结束会被删除
        mapRDD.checkpoint()  // 需要落盘，需要指定检查点路径，作业结束不会被删除

        val reduceRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_ + _)

        reduceRDD.collect().foreach(println)

        println("=================================================")

        val groupRDD: RDD[(String, Iterable[Int])] = mapRDD.groupByKey()

        groupRDD.collect().foreach(println)


        // 关闭spark
        sc.stop()
    }


}
