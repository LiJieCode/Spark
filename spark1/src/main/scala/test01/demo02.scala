package test01

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
 *
 * SparkSQL
 * - DataFrame
 *   - SQL
 *     - 先创建一个视图
 *     - 直接写SQL，就可以操作了
 *   - DSL
 *     - 直接用df.xxx
 *     -
 *
 * - DataSet
 *
 */


object demo02 {
    def main(args: Array[String]): Unit = {
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("demo01")
        val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
        import spark.implicits._

        // 读取的数据有两个字段
        // id, name
        val df: DataFrame = spark.read.json("datas/data.json")


        val DSLdf: DataFrame = df.select("id")
        val DSLdf1: DataFrame = df.drop("id")

        DSLdf1.show()

        spark.close()

    }

    // 样例类
    case class User(id: Int, name: String)
}
