package test01

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 *
 * SparkSQL - 创建DataFrame
 *
 */


object demo00 {
    def main(args: Array[String]): Unit = {
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("demo01")
        val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
        import spark.implicits._

        // 创建1 - 读取的数据有两个字段
        // id, name
        val df: DataFrame = spark.read.json("datas/data.json")

        // 创建2
        val ints = List(1, 2, 3, 4)
        val df1: DataFrame = ints.toDF()

        // 创建3
        val users = List(User(1, "zhang1"), User(2, "zhang2"))
        val df2: DataFrame = users.toDF()

        // 创建4 - RRD 和 DF 和 DS 的互相转换



        df2.show()
        spark.close()

    }
    // 样例类
    case class User(id: Int, name: String)
}
