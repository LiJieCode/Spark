package test01

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
 *
 *  SQL 语法
 *
 *
 */
object Demo05 {
    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local").setAppName("05131")

        val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()

        import spark.implicits._

        val df: DataFrame = spark.read.json("data/data.json")

        // val ds: Dataset[User] = df.as[User]

        // 创建一个视图
        // df.createOrReplaceTempView("data")

        // 直接写SQL
        val df1: DataFrame = spark.sql("select * from data where id = 1 and id = 2")

        df1.show()

        spark.close()
    }

    case class User(id: Int, name: String)
}
