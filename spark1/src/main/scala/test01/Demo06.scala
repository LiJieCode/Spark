package test01

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
 *
 * DLS 语法
 *
 */
object Demo06 {
    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local").setAppName("0513")

        val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()




    }

}
