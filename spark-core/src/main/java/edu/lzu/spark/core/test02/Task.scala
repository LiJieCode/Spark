package edu.lzu.spark.core.test02

class Task extends Serializable{
    // 定义数据
    private val datas: List[Int] = List(1, 2, 3, 4)

    // 定义逻辑
    // private val logic = (num: Int) => { num * 2 }
    private val logic: (Int => Int) = _ * 2

    // 定义计算的方法
    def compute() = {
        datas.map(logic)
    }

}
