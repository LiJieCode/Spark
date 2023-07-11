package regularScala
import scala.util.matching.Regex

object Demo01 {
    def main(args: Array[String]): Unit = {
        
        val content1 = "hello, i am LEGOTIME, my blog site is 123456789, hello everyone!"
        // val content1 = "hello, i am LEGOTIME, my blog site is http://blog.csdn.net/legotime, hello everyone!"
        var content2 = "Scala is scalable and cool, Scala is scala"
        
        // var regex1 = "hello".r
    
        // todo 第一种用法，匹配
        // content1.matches()  返回
        
        println(content1.matches("\\w+, (\\w+ ){2,}\\w+, (\\w+ ){2,}\\d+, hello everyone!"))
        
        
        // todo 第二种用法，
        val regex1: Regex = "Scala".r
        val regex2: Regex = "(s|S)cala".r
    
        // println(regex1 findFirstIn content2)
        
        val iterator: Regex.MatchIterator = regex1 findAllIn content2
        while (iterator.hasNext){
            println(iterator.next())
        }
    
        println(regex2.findAllIn(content2).mkString(","))
        
        
        // todo 第三种用法，替换
        // regex1.replaceSomeIn()  还没研究怎么用
        println(regex1.replaceAllIn(content2, "Java"))
        println(regex1.replaceFirstIn(content2, "Java"))
        
        
    }
    
}
