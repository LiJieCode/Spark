package test05Hive

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object Spark01Hive {
    def main(args: Array[String]): Unit = {

        // System.setProperty("HADOOP_USER_NAME", "lijzh")

        // TODO 创建SparkSQL的运行环境

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("SQL")
        // 增加了.enableHiveSupport()
        val spark: SparkSession = SparkSession.builder()
          .enableHiveSupport()
          .config(sparkConf)
          .getOrCreate()

        // ToDO 使用SparkSQL连接外置Hive
        // 1 拷贝 Hive-site.xml 到 resources
        // 2 启动 Hive 的支持
        // 3 增加对应的依赖关系，包含mysql驱动

        spark.sql("show databases").show()


        // TODO 关闭环境
        spark.close()



    }

}
