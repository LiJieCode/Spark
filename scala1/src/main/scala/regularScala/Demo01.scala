package regularScala

object Demo01 {
    def main(args: Array[String]): Unit = {
        
        val content1 = "hello, i am LEGOTIME, my blog site is 123456789, hello everyone!"
        // val content1 = "hello, i am LEGOTIME, my blog site is http://blog.csdn.net/legotime, hello everyone!"
        
        // var regex1 = "hello".r
    
        // todo 第一种
        println(content1.matches("\\w+, (\\w+ ){2,}\\w+, (\\w+ ){2,}\\d+, hello everyone!"))
        
        // todo 第二种
        
    }
    
}
