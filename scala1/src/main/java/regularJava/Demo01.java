package regularJava;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 *
 * https://www.runoob.com/java/java-regular-expressions.html
 *
 */
public class Demo01 {
    public static void main(String[] args) {
        String content1 = "HEllo Java";
    
        // todo 第一种 Pattern.matches 匹配
        System.out.println(Pattern.matches("[A-Z]{1}[a-z]+ [A-Z]{1}\\w+", content1));
        // false
        
        String content2 = "This order was placed for QT3000! OK?";
    
        // todo 第一种
        System.out.println(Pattern.matches("(\\w+ ){2,}\\w+! \\w+?", content2));
    
        
        // todo 第二种 Pattern.compile
        Pattern r = Pattern.compile("(\\D*)(\\d+)(.*)");
        Matcher m = r.matcher(content2);
        
        if (m.find()){
            System.out.println(m.group(0));
            System.out.println(m.group(1));
            System.out.println(m.group(2));
            System.out.println(m.group(3));
        }
    }
}
