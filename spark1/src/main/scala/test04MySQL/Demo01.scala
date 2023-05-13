package test04MySQL

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

/**
 *
 * 连接mysql
 *
 * 读取sql,以及保存数据到sql
 *
 */
object Demo01 {

    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("demo01_1_1")
        val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
        import spark.implicits._

        val df: DataFrame = spark.read.format("jdbc")
          .option("url", "jdbc:mysql://127.0.0.1:3306/db_spark?useSSL=false&serverTimezone=UTC")
          .option("driver", "com.mysql.cj.jdbc.Driver")
          .option("user", "root")
          .option("password", "li123...")
          .option("dbtable", "account")
          .load()

        df.createOrReplaceTempView("account")

        val resDF1: DataFrame = spark.sql(
            """
              |select * from account
              |""".stripMargin)

        // todo 写开窗函数也没问题
        val resDF: DataFrame = spark.sql(
            """
              |select
              |    dept,
              |    salary,
              |    rank() over(partition by dept order by salary) as rk
              |from account
              |""".stripMargin)

        resDF.show()







        spark.close()


    }

}
