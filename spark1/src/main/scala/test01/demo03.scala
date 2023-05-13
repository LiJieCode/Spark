package test01

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
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
 *   - 通过样例类创建DataSet，通过集合创建
 *
 *
 */


object demo03 {
    def main(args: Array[String]): Unit = {
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("demo01")
        val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
        import spark.implicits._

        // 创建1 - 通过样例类
        val users: Seq[User] = Seq(User(1, "zhang1"), User(2, "zhang2"))
        val ds: Dataset[User] = users.toDS()

        // 创建2 - 通过集合
        val tuples = List((1, 2), (2, 3), (3, 4))
        val ds1: Dataset[(Int, Int)] = tuples.toDS()




        spark.close()
    }
    // 样例类
    case class User(id: Int, name: String)
}
