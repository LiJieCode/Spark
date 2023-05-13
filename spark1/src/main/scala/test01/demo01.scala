package test01

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 *
 * SparkSQL
 * - DataFrame
 *   - SQL
 *     - 先创建一个视图
 *     - 直接写SQL，就可以操作了
 *
 * - DataSet
 *
 */


object demo01 {
    def main(args: Array[String]): Unit = {
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("demo01")
        val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
        import spark.implicits._

        // 读取的数据有两个字段
        // id, name
        val df: DataFrame = spark.read.json("datas/data.json")

        // val rdd: RDD[Int] = spark.sparkContext.makeRDD(List(1, 2, 3, 4))
        // val df1 = rdd.toDF("id")

        df.createOrReplaceTempView("user")
        // 临时窗口不支持update
        // DELETE is only supported with v2 tables
        val SQLdf: DataFrame = spark.sql("select * from user where id = 1")


        SQLdf.show()

        spark.close()

    }

}
