package test02UDF

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 *
 * UDF - 简单的
 *
 */
object Demo01 {
    def main(args: Array[String]): Unit = {
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("demo01_1")
        val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
        import spark.implicits._


        val df: DataFrame = spark.read.json("datas/data.json")

        df.createOrReplaceTempView("temp")

        spark.udf.register("addName", "Name:" + _)

        val dfSQL: DataFrame = spark.sql("select id, addName(name) from temp")

        dfSQL.show()


        spark.close()

    }

}
