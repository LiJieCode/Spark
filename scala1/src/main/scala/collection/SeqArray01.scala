package collection

/**
 *
 * 不可变数组Array
 *
 */
object SeqArray01 {
    def main(args: Array[String]): Unit = {
        // todo 定义数组
        val ints1 = new Array[Int](5)
        val ints: Array[Int] = Array(1, 2, 3, 4)


        // todo 数组赋值
        ints1(0) = 1


        // todo 数组遍历
        // 查看数组
        println(ints.mkString(","))

        // 循环简单遍历
        for (ele <- ints){
            // println(ele)
        }

        // 简化遍历
        ints.foreach(println)


    }
}
