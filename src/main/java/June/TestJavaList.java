package June;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * @Description TODO
 * @Author yumigzhu
 * @Date 2019/6/17 15:35
 */
public class TestJavaList {
    public static void main(String[] args) {
        List<Integer>  list=new ArrayList<>();
        list.add(1);
        list.add(3);
        list.add(2);
//        Collections.sort(list);
        Arrays.sort(list.toArray());
        for (Integer integer : list) {
            System.out.println(integer);
        }


    }
}
