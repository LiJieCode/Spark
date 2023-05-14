package collection

object Map01 {
    def main(args: Array[String]): Unit = {

        val map = Map(1 -> "zhang1", 2 -> "zhang2")

        val str1: String = map.mkString(",")

        println(str1)
    }

}
