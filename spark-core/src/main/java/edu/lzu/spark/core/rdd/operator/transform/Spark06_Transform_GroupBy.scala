package edu.lzu.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark06_Transform_GroupBy {
    def main(args: Array[String]): Unit = {

        // 连接spark
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("transform")
        val sc = new SparkContext(sparkConf)


        // TODO 算子 - GroupBy
        val rdd: RDD[Int] = sc.makeRDD(
            List(1,2,3,4), 2
        )

        // groupFunction 用来将数据源中的每一个数据进行分组判断
        // 根据返回的key进行分组，相同的key放在一组
        def groupFunction(num: Int): Int ={
            num % 2
        }

        val groupRDD: RDD[(Int, Iterable[Int])] = rdd.groupBy(groupFunction)

        groupRDD.collect().foreach(println)


        // 关闭spark
        sc.stop()

    }
}
