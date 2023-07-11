package anli.again

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object XuQiu01 {

  def main(args: Array[String]): Unit = {

    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("0101")
    val sc = new SparkContext(sparkConf)
    val RDD: RDD[String] = sc.textFile("data/user_visit_action.txt")
    
    
    // 第一步：统计点击数量
    // 1.1：过滤出来点击的数据
    val clickRDD: RDD[String] = RDD.filter(
      line => {
        val datas: Array[String] = line.split("_")
        datas(6) != "-1"
      }
    )
    // 1.2: map
    val clickMapRDD: RDD[(String, Int)] = clickRDD.map(
      line => {
        val datas: Array[String] = line.split("_")
        (datas(6), 1)
      }
    )
    // 1.3: reduce
    val clickReduceRDD: RDD[(String, Int)] = clickMapRDD.reduceByKey(_+_)
    // clickReduceRDD.foreach(println)
    
    
    // 第二步：统计下单的数量
    // 2.1 过滤数据
    val orderRDD: RDD[String] = RDD.filter(
      line => {
        val datas: Array[String] = line.split("_")
        datas(8) != null
      }
    )
    // 2.2 统计
    val orderReduceRDD: RDD[(String, Int)] = orderRDD.flatMap(
      line => {
        val datas: Array[String] = line.split("_")
        datas(8).split(",")
      }
    ).map(
      (_, 1)
    ).reduceByKey(_ + _)
    
    
    
    // 第三步：合并
    val coRDD: RDD[(String, (Iterable[Int], Iterable[Int]))] = clickReduceRDD.cogroup(orderReduceRDD)
    
    
    // 第四步：改变数据格式
    val ansRDD: RDD[(String, (Int, Int))] = coRDD.mapValues {
      case (clickIter, orderIter) => {
        var clickNum = 0
        val iter1: Iterator[Int] = clickIter.iterator
        if (iter1.hasNext){
          clickNum = iter1.next()
        }
        // val orderNum = orderIter.iterator.next()
  
        var orderNum = 0
        val iter2: Iterator[Int] = orderIter.iterator
        if (iter2.hasNext) {
          orderNum = iter2.next()
        }
        (clickNum, orderNum)
      }
    }
    
    // 第五步：排序(元组排序)
    // val res: Array[(String, (Int, Int))] = ansRDD.sortBy(_._2, false).take(10)
    val res: RDD[(String, (Int, Int))] = ansRDD.sortBy(_._2, false)
    
    res.collect().foreach(println)
    
    
    
    
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


