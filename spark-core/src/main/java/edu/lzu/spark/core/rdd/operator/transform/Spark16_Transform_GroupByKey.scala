package edu.lzu.spark.core.rdd.operator.transform

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Spark16_Transform_GroupByKey {
    def main(args: Array[String]): Unit = {

        // 连接spark
        val sparkConf = new SparkConf().setMaster("local[*]").setAppName("transform")
        val sc = new SparkContext(sparkConf)


        // TODO 算子 - GroupByKey
        val rdd: RDD[(String, Int)] = sc.makeRDD(
            List(
                ("a", 1), ("a", 2), ("b", 3), ("b", 4), ("c", 5)
            )
        )

        // groupByKey 将数据源中，相同key的数据分在一个组内
        // 返回值，元组中第一个元素就是key，第二个元素就是相同key的value的集合
        val groupRDD: RDD[(String, Iterable[Int])] = rdd.groupByKey()
        groupRDD.collect().foreach(println)

        // groupByKey 和 reduceByKey 的区别  -  P67
        //
        // 从 shuffle 的角度： reduceByKey 和 groupByKey 都存在 shuffle 的操作，但是 reduceByKey
        // 可以在 shuffle 前对分区内相同 key 的数据进行预聚合（combine）功能，这样会减少落盘的
        // 数据量，而 groupByKey 只是进行分组，不存在数据量减少的问题， reduceByKey 性能比较高。
        // 从功能的角度： reduceByKey 其实包含分组和聚合的功能。 GroupByKey 只能分组，不能聚
        // 合，所以在分组聚合的场合下，推荐使用 reduceByKey，如果仅仅是分组而不需要聚合。那
        // 么还是只能使用 groupByKey
        //
        //
        // reduceByKey 支持分区内预聚合，可以有效地减少shuffle时落盘的数据量
        //
        // reduceByKey 效率比 groupByKey 高
        //
        // spark中，shuffle操作必须罗盘处理，不能在内存中数据等待，会导致内存溢出。
        // 因此shuffle的性能非常低，要和硬盘交互。
        val groupRDD1: RDD[(String, Iterable[(String, Int)])] = rdd.groupBy(_._1)
        groupRDD1.collect().foreach(println)


        // 关闭spark
        sc.stop()
    }
}
