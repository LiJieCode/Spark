package edu.lzu.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark12_Transform_SortBy {
    def main(args: Array[String]): Unit = {

        // 连接spark
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("transform")
        val sc = new SparkContext(sparkConf)


        // TODO 算子 - SortBy
        val rdd: RDD[Int] = sc.makeRDD(
            List(2,6,3,7,4,1,8)
        )

        // SortBy保持分区数不变，所以必须经过了shuffle处理
        // 第二个参数，默认是true是升序排，修改为false就是降序了
        val newRDD: RDD[Int] = rdd.sortBy(num => num)

        newRDD.collect().foreach(println)




        // 关闭spark
        sc.stop()

    }
}
