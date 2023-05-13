package test03

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 *
 * 数据的读取和保存
 *
 */
object demo01 {
    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("demo01_1_1")
        val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
        import spark.implicits._


        val df: DataFrame = spark.read.format("json").load("datas/data.json")

        // df.show()

        df.write.json("output")


        spark.close()



    }

}
