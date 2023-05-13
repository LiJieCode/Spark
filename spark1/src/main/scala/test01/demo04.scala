package test01

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Dataset, SparkSession}

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
 *   - 通过样例类创建DataSet，通过集合创建
 *
 * - DF DS RDD 之间的转换
 *
 */


object demo04 {
    def main(args: Array[String]): Unit = {
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("demo01")
        val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
        import spark.implicits._



        spark.close()
    }
    // 样例类
    case class User(id: Int, name: String)
}
