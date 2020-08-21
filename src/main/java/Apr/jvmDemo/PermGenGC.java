package Apr.jvmDemo;

/**
 * @Description TODO
 * @Author yumigzhu
 * @Date 2020/4/2 11:42
 */
public class PermGenGC {
    public static void main(String[] args) {
        for (int i = 0; i <Integer.MAX_VALUE ; i++) {
            String t=String.valueOf(i).intern();
        }


    }
}
