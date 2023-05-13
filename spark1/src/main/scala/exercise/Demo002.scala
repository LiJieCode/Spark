package exercise

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

/**
 *
 * RDD  DataFrame  DateSet 之间的相互转换
 */
object Demo002 {
    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local").setAppName("0513")
        val spark: SparkSession = SparkSession.builder().config(sparkConf).getOrCreate()
        import spark.implicits._

        // todo 创建RDD
        val rdd: RDD[(Int, String)] = spark.sparkContext.makeRDD(
            List((1, "zhang1"), (2, "zhang2"), (3, "zhang3"))
        )
        // rdd.foreach(a => println(a))
        // rdd.foreach(println)
        // rdd.collect().foreach(println)

        // todo  RDD -> DateFrame
        // val df: DataFrame = rdd.toDF()  // 列名是默认的
        val df: DataFrame = rdd.toDF("id", "name")   // 指定列名
        // df.show()

        // todo DataFrame -> DataSet
        val ds: Dataset[User] = df.as[User]
        // ds.show()

        // DataFrame 和 DataSet 的区别

        // todo DataSet -> DataFrame
        val df1: DataFrame = ds.toDF()
        // df1.show()

        // todo DataFrame -> RDD
        //  返回的 RDD 类型为 Row，里面提供的 getXXX 方法可以获取字段值，类似 jdbc 处理结果集，但是索引从 0 开始
        val rdd1: RDD[Row] = df1.rdd
        // rdd1.foreach(a=>println(a.getString(1)))   // a 代表是一行数据
        // rdd1.collect().foreach(println)


        // todo RDD -> DataSet
        // val ds1: Dataset[(Int, String)] = rdd.toDS()
        // ds1.show()
        val mapRDD: RDD[User] = rdd.map {
            case (id, name) => User(id, name)
        }
        val ds1: Dataset[User] = mapRDD.toDS()
        // ds1.show()


        // todo DataSet -> RDD
        val rdd2: RDD[User] = ds1.rdd
        rdd2.foreach(
            u => {
                println(u.id + " " + u.name)
            }
        )


        spark.close()
    }
    case class User(id: Int, name: String)
}
