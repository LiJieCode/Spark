package test02

import org.apache.spark.SparkConf

object Demo001 {
    def main(args: Array[String]): Unit = {
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("0511")



    }
}
