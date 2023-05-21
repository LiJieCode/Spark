package anli

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}


/**
 *
 * 再次优化需求一：
 *
 */
object Demo04 {

    def main(args: Array[String]): Unit = {
        // TODO : Top10热门品类
        val sparConf = new SparkConf().setMaster("local[*]").setAppName("HotCategoryTop10Analysis")
        val sc = new SparkContext(sparConf)


        // Q : 存在大量的shuffle操作（reduceByKey）
        // reduceByKey 聚合算子，spark会提供优化，缓存

        // 1. 读取原始日志数据
        val actionRDD: RDD[String] = sc.textFile("data/user_visit_action.txt")

        // 2. 将数据转换结构
        //    点击的场合 : ( 品类ID，( 1, 0, 0 ) )
        //    下单的场合 : ( 品类ID，( 0, 1, 0 ) )
        //    支付的场合 : ( 品类ID，( 0, 0, 1 ) )
        val flatRDD: RDD[(String, (Int, Int, Int))] = actionRDD.flatMap(
            action => {
                val datas: Array[String] = action.split("_")
                if (datas(6) != "-1") {
                    // 点击的场合
                    List((datas(6), (1, 0, 0)))    // 这里为什么是列表？？？？？？？？？？？？？/
                } else if (datas(8) != "null") {
                    // 下单的场合
                    val ids: Array[String]  = datas(8).split(",")
                    ids.map(id => (id, (0, 1, 0)))
                } else if (datas(10) != "null") {
                    val ids: Array[String] = datas(10).split(",")
                    // 支付的场合
                    ids.map(id => (id, (0, 0, 1)))
                } else {
                    Nil
                }
            }
        )

        // 3. 将相同的品类ID的数据进行分组聚合
        //    ( 品类ID，( 点击数量, 下单数量, 支付数量 ) )
        val analysisRDD: RDD[(String, (Int, Int, Int))] = flatRDD.reduceByKey(
            (t1, t2) => {
                (t1._1 + t2._1, t1._2 + t2._2, t1._3 + t2._3)
            }
        )

        // 4. 将统计结果根据数量进行降序处理，取前10名
        val resultRDD = analysisRDD.sortBy(_._2, false).take(10)

        // 5. 将结果采集到控制台打印出来
        resultRDD.foreach(println)

        sc.stop()
    }


    // 样例类
    case class UserVisitAction(
                                date: String, //用户点击行为的日期
                                user_id: Long, //用户的 ID
                                session_id: String, //Session 的 ID
                                page_id: Long, //某个页面的 ID
                                action_time: String, //动作的时间点
                                search_keyword: String, //用户搜索的关键词
                                click_category_id: Long, //某一个商品品类的 ID
                                click_product_id: Long, //某一个商品的 ID
                                order_category_ids: String, //一次订单中所有品类的 ID 集合
                                order_product_ids: String, //一次订单中所有商品的 ID 集合
                                pay_category_ids: String, //一次支付中所有品类的 ID 集合
                                pay_product_ids: String, //一次支付中所有商品的 ID 集合
                                city_id: Long
                              ) //城市 id

}
