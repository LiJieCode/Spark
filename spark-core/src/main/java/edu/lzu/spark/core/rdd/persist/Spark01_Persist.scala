package edu.lzu.spark.core.rdd.persist

import org.apache.spark.rdd.RDD
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object Spark01_Persist {
    def main(args: Array[String]): Unit = {

        // 连接spark
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("transform")
        val sc = new SparkContext(sparkConf)


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
        mapRDD.persist(StorageLevel.DISK_ONLY)   // 将mapRDD数据临时存到磁盘中

        val reduceRDD: RDD[(String, Int)] = mapRDD.reduceByKey(_ + _)

        reduceRDD.collect().foreach(println)

        println("=================================================")

        val groupRDD: RDD[(String, Iterable[Int])] = mapRDD.groupByKey()

        groupRDD.collect().foreach(println)


        // 关闭spark
        sc.stop()
    }


}
