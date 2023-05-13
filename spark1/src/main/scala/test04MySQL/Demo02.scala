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
object Demo02 {

    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("demo01_1_1")
        val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
        import spark.implicits._

        val df: DataFrame = spark.read.format("jdbc")
          .option("url", "jdbc:mysql://127.0.0.1:3306/db_spark?useSSL=false&serverTimezone=UTC")
          .option("driver", "com.mysql.cj.jdbc.Driver")
          .option("user", "root")
          .option("password", "li123...")
          .option("dbtable", "user")
          .load()

        // df.show()

        //
        df.write.format("jdbc")
          .option("url", "jdbc:mysql://127.0.0.1:3306/db_spark?useSSL=false&serverTimezone=UTC")
          .option("driver", "com.mysql.cj.jdbc.Driver")
          .option("user", "root")
          .option("password", "li123...")
          .option("dbtable", "user1")
          .mode(SaveMode.Append)
          .save()


        spark.close()


    }

}
