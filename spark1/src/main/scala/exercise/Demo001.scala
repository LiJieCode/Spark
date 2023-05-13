package exercise

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SparkSession}

object Demo001 {
    def main(args: Array[String]): Unit = {
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("0511")

        val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
        import spark.implicits._

        val rdd: RDD[Int] = spark.sparkContext.makeRDD(
            List(1, 2, 3, 4)
            //Map()
        )

        val df: DataFrame = rdd.toDF("id")

        df.show()

        spark.close()
    }

    case class User(id: Int, name: String)
}
