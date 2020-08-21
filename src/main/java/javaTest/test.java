package javaTest;

/**
 * @Description TODO
 * @Author yumigzhu
 * @Date 2019/3/4 18:57
 */
public class test {
    public static void main(String[] args) {
        StringBuilder str=new StringBuilder();
        int i=18675;
        str.append("[");
        while  (i<=18788){
            str.append(i).append(",");
                    i++;
        }
        str.append("]");
        System.out.println(str.toString());
    }
}
