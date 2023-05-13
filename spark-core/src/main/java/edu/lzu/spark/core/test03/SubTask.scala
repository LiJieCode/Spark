package edu.lzu.spark.core.test03

class SubTask extends Serializable{
    // 定义数据
    var datas: List[Int] = _

    // 定义逻辑
    // private val logic = (num: Int) => { num * 2 }
    var logic: Int => Int = _

    def compute() = {
        datas.map(logic)
    }

}
