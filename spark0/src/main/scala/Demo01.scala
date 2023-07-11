import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Demo01 {
    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("0512")
        val sc = new SparkContext(sparkConf)


        val rdd: RDD[String] = sc.textFile("data/words.txt")

        val value: RDD[String] = rdd.filter(
            line => {
                // val datas: Array[String] = line.split(" ")
                // line.matches("[a-z]+( )[A-Z]+")
                // line.matches("[A-Z]{1,2}[a-zA-Z0-9]+ [A-Z]{1}[a-zA-Z0-9]+")
                line.matches("[A-Z]{1,2}\\w* [A-Z]{1}\\w*")

            }
        )

        value.collect().foreach(println)

        sc.stop()
    }

}
