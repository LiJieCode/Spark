import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Demo01 {
    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("0512")

        val sc = new SparkContext(sparkConf)

        val rdd: RDD[String] = sc.textFile("datas/words.txt")

        val flatRdd: RDD[String] = rdd.flatMap(_.split(" "))


    }

}
