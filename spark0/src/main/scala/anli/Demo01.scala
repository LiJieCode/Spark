package anli

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object Demo01 {

    def main(args: Array[String]): Unit = {

        //
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("0512")
        val sc = new SparkContext(sparkConf)

        // 1、读取文件数据
        val rdd: RDD[String] = sc.textFile("data/user_visit_action.txt")


        // 2、统计品类的点击数量
        // 2.1 先过去出点击的数据
        rdd.filter(
            line => {
                val datas: Array[String] = line.split("_")
                datas(6) != "-1" // 等于-1，表示不是点击数据
            }
        )





        // 3、





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
