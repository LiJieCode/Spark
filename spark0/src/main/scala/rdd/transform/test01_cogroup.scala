package rdd.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object test01_cogroup {

    def main(args: Array[String]): Unit = {
        // 连接spark
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("transform")
        val sc = new SparkContext(sparkConf)

        val rdd1: RDD[(Char, Int)] = sc.makeRDD(
            List(('a', 1), ('b', 2), ('c', 3))
        )

        val rdd2: RDD[(Char, Int)] = sc.makeRDD(
            List(('a', 11), ('b', 22), ('d', 44))
        )

        // cogroup = connect + group
        val rdd12: RDD[(Char, (Iterable[Int], Iterable[Int]))] = rdd1.cogroup(rdd2)

        rdd12.collect().foreach(println)

        sc.stop()

    }
}
