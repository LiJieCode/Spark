package test01

import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}


/**
 *
 * kafka 数据源 ，贼重要
 * P192
 *
 */
object Spark04_kafka {
    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("0909")
        val ssc = new StreamingContext(sparkConf, Seconds(2))

        // 定义kafka参数  kafkaPara
        val kafkaPara: Map[String, Object] = Map[String, Object](
            ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "l9z102:9092,l9z102:9092,l9z102:9092",
            // 消费者组
            ConsumerConfig.GROUP_ID_CONFIG -> "atguigu",
            "key.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer",
            "value.deserializer" -> "org.apache.kafka.common.serialization.StringDeserializer"
        )


        // 读取 Kafka 数据创建 DStream
        val kafkaData: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream[String, String](
            ssc,
            LocationStrategies.PreferConsistent,
            ConsumerStrategies.Subscribe[String, String](Set("topic_name"), kafkaPara)
        )

        // 将每条消息的 KV 取出
        // kafkaData.map(_.key())
        val kafkaValue: DStream[String] = kafkaData.map(_.value())

        kafkaData.print()

        // 计算


        // 开启任务
        ssc.start()
        ssc.awaitTermination()
    }
}
