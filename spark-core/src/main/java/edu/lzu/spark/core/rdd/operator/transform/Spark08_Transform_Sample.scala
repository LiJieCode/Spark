package edu.lzu.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark08_Transform_Sample {
    def main(args: Array[String]): Unit = {

        // 连接spark
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("transform")
        val sc = new SparkContext(sparkConf)


        // TODO 算子 - Sample
        val rdd: RDD[Int] = sc.makeRDD(
            List(1,2,3,4,5,6,7,8,9)
        )

        // sample（抽样） 有三个参数
        // 第一个参数，是否放回，true表示放回
        // 第二个参数，
        //         如果第一个参数是 false，那么第二个参数代表每条数据被抽取的概率
        //         如果第一个参数是 true，那么第二个参数代表每条数据可能抽取的次数
        // 第三个参数，抽取数据时随机算法的种子(如果不传入第三个参数，默认采用系统时间戳为随机种子）
        // 随机种子一旦确定，那么每个数据的概率就确定了，只有大于我们给的第二个参数(0.4)才被抽出来
        val sampleRDD: RDD[Int] = rdd.sample(
            false, 0.4
        )

        // sampleRDD.collect().foreach(println)

        println(sampleRDD.collect().mkString(","))


        // 关闭spark
        sc.stop()

    }
}
