package test02UDF

import org.apache.spark.SparkConf
import org.apache.spark.sql.expressions.{Aggregator, UserDefinedAggregateFunction}
import org.apache.spark.sql.{DataFrame, Encoder, Encoders, SparkSession, functions}

/**
 *
 * UDAF - 自定义
 *
 */
object Demo02 {
    def main(args: Array[String]): Unit = {

        // TODO 创建SparkSQL的运行环境
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("sparkSQL")
        val spark = SparkSession.builder().config(sparkConf).getOrCreate()

        val df = spark.read.json("datas/data.json")
        df.createOrReplaceTempView("user")

        // 注册一个UDF函数
        spark.udf.register("ageAvg", functions.udaf(new MyAvgUDAF()))

        // SQL语句
        // 弱类型的操作
        val SQLdf: DataFrame = spark.sql("select ageAvg(id) from user")

        SQLdf.show()

        // TODO 关闭环境
        spark.close()
    }

    /**
     * 自定义聚合函数类：计算年龄的平均值
     *  1. 继承 org.apache.spark.sql.expressions.Aggregator, 定义泛型
     *     IN : 输入的数据类型 Long
     *     BUF : 缓冲区的数据类型 Buff
     *     OUT : 输出的数据类型 Long
     *     2. 重写方法(6)
     */
    // 样例类
    // 样例类可以通过类名直接创建对象
    case class Buff(var total: Long, var count: Long)

    // 强类型的UDAF,可以通过属性名访问
    // 弱类型，通过下标访问
    class MyAvgUDAF extends Aggregator[Long, Buff, Long]{
        // buff 初始值
        override def zero: Buff = {
            Buff(0L,0L)
        }

        // 根据输入的数据更新缓冲区的数据
        override def reduce(buff: Buff, input: Long): Buff = {
            buff.total += input
            buff.count += 1
            buff
        }

        // 分区合并
        override def merge(buff1: Buff, buff2: Buff): Buff = {
            buff1.total += buff2.total
            buff1.count += buff2.count
            buff1
        }

        // 计算结果
        override def finish(buff: Buff): Long = buff.total / buff.count

        // 缓冲区编码
        // 自定义的数据类型，就是这样写Encoders.product
        override def bufferEncoder: Encoder[Buff] = Encoders.product
        // 输出结果的编嘛
        // scala中自带的数据类型，就是这样写Encoders.scalaXXX
        override def outputEncoder: Encoder[Long] = Encoders.scalaLong
    }


//    // class MyAvgUDAF extends UserDefinedAggregateFunction
//    class MyAvgUDAF extends Aggregator[Long, Buff, Long] {
//
//        // z & zero : 初始值或零值
//        // 缓冲区的初始化
//        override def zero: Buff = {
//            Buff(0L, 0L)
//        }
//
//        // 根据输入的数据更新缓冲区的数据
//        override def reduce(buff: Buff, in: Long): Buff = {
//            buff.total = buff.total + in
//            buff.count = buff.count + 1
//            buff
//        }
//
//        // 合并缓冲区
//        override def merge(buff1: Buff, buff2: Buff): Buff = {
//            buff1.total = buff1.total + buff2.total
//            buff1.count = buff1.count + buff2.count
//            buff1
//        }
//
//        //计算结果
//        override def finish(buff: Buff): Long = {
//            buff.total / buff.count
//        }
//
//        // 缓冲区的编码操作
//        override def bufferEncoder: Encoder[Buff] = Encoders.product
//
//        // 输出的编码操作
//        override def outputEncoder: Encoder[Long] = Encoders.scalaLong
//    }


}