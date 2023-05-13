package edu.lzu.spark.core.test03

class Task extends Serializable{
    // 定义数据
    val datas: List[Int] = List(1, 2, 3, 4)

    // 定义逻辑
    // private val logic = (num: Int) => { num * 2 }
    val logic: Int => Int = _ * 2


}
