package anli

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
 *
 * 需求一：
 *
 */
object Demo01 {

    def main(args: Array[String]): Unit = {

        //
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("0512")
        val sc = new SparkContext(sparkConf)

        // 1、读取文件数据
        val rdd: RDD[String] = sc.textFile("data/user_visit_action.txt")


        // 2、统计品类的点击数量
        // 2.1 先过滤出点击的数据
        val clickRDD: RDD[String] = rdd.filter(
            line => {
                val datas: Array[String] = line.split("_")
                // filter 等于true的时候返回
                datas(6) != "-1"     // 等于-1，表示不是点击数据
            }
        )
        // 2.2 统计
        val mapClickRDD: RDD[(String, Int)] = clickRDD.map(
            line => {
                val datas: Array[String] = line.split("_")
                (datas(6), 1)
            }
        )
        val clickWC: RDD[(String, Int)] = mapClickRDD.reduceByKey(_ + _)


        // 3、统计品类的下单数量
        // 3.1 先过滤出下单的数据
        val orderRDD: RDD[String] = rdd.filter(
            line => {
                val datas: Array[String] = line.split("_")
                datas(8) != "null"
            }
        )
        // 3.2 统计
        val flatOrderRDD: RDD[String] = orderRDD.flatMap(
            line => {
                val datas: Array[String] = line.split("_")
                datas(8).split(",")
            }
        )
        val orderWC: RDD[(String, Int)] = flatOrderRDD.map((_, 1)).reduceByKey(_ + _)

        // orderWC.collect().foreach(println)


        // 4、统计品类的支付数量
        // 4.1 先过滤出支付的数据
        val payRDD: RDD[String] = rdd.filter(
            line => {
                val datas: Array[String] = line.split("_")
                // filter 等于true的时候返回
                datas(10) != "null" // 等于-1，表示不是点击数据
            }
        )
        // 4.2 统计
        val payWC: RDD[(String, Int)] = payRDD.flatMap(
            line => {
                val datas: Array[String] = line.split("_")
                val arr: Array[String] = datas(10).split(",")
                arr.map((_, 1))
            }
        ).reduceByKey(_ + _)

        // payWC.foreach(println)

        // 5、将品类进行排序，并取前10名
        // 点击量排序，下单量排序，支付量排序
        // 利用元组排序，先比较第一个，再比较第二个，...

        val cgRDD: RDD[(String, (Iterable[Int], Iterable[Int], Iterable[Int]))] = clickWC.cogroup(orderWC, payWC)

        val ansRDD: RDD[(String, (Int, Int, Int))] = cgRDD.mapValues {
            case (clickIter, orderIter, payIter) => {
                var clickNum = 0
                val iter1: Iterator[Int] = clickIter.iterator
                if (iter1.hasNext) {
                    clickNum = iter1.next()
                }

                var orderNum: Int = 0
                val iter2: Iterator[Int] = orderIter.iterator
                if (iter2.hasNext) {
                    orderNum = iter2.next()
                }

                var payNum: Int = 0
                val iter3: Iterator[Int] = payIter.iterator
                if (iter3.hasNext) {
                    payNum = iter3.next()
                }

                (clickNum, orderNum, payNum)
            }
        }

        val result: Array[(String, (Int, Int, Int))] = ansRDD.sortBy(_._2, false).take(10)

        result.foreach(println)

        sc.stop()
    }

    // 样例类
    case class UserVisitAction(
                                date: String,//用户点击行为的日期
                                user_id: Long,//用户的 ID
                                session_id: String,//Session 的 ID
                                page_id: Long,//某个页面的 ID
                                action_time: String,//动作的时间点
                                search_keyword: String,//用户搜索的关键词
                                click_category_id: Long,//某一个商品品类的 ID
                                click_product_id: Long,//某一个商品的 ID
                                order_category_ids: String,//一次订单中所有品类的 ID 集合
                                order_product_ids: String,//一次订单中所有商品的 ID 集合
                                pay_category_ids: String,//一次支付中所有品类的 ID 集合
                                pay_product_ids: String,//一次支付中所有商品的 ID 集合
                                city_id: Long
                              )//城市 id

}
