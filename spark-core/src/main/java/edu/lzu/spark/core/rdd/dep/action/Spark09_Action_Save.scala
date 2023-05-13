package edu.lzu.spark.core.rdd.dep.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark09_Action_Save {
    def main(args: Array[String]): Unit = {

        // 连接spark
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("transform")
        val sc = new SparkContext(sparkConf)

        // TODO 算子 - Save
        //
        val rdd: RDD[(String, Int)] = sc.makeRDD(List(
            ("a", 1), ("a", 3), ("a", 2)
        ))


        rdd.saveAsTextFile("output")
        rdd.saveAsObjectFile("output1")

        // saveAsSequenceFile 要求数据必须是K-V类型
        rdd.saveAsSequenceFile("output2")



        // 关闭spark
        sc.stop()

    }
}
