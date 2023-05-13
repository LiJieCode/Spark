package edu.lzu.spark.core.rdd.dep.action

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark10_Action_Test {
    def main(args: Array[String]): Unit = {

        // 连接spark
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("transform")
        val sc = new SparkContext(sparkConf)

        // TODO 算子 - Foreach
        //
        val rdd: RDD[Int] = sc.makeRDD(List(
            1, 2, 3, 4
        ))

        val user = new User
        rdd.foreach(
            num =>{
                println(num + user.age)
            }
        )


        // 关闭spark
        sc.stop()
    }



    // Driver 生成的对象，要在Executor使用，对象必须要可序列化
    // 样例类在编译时，自动混入序列化特质（实现可序列化接口）
    // case class User(){}
    class User extends Serializable {
        var age: Int = 30
    }


}
