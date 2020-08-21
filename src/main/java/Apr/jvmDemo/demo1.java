package Apr.jvmDemo;

import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @Description TODO
 * @Author yumigzhu
 * @Date 2020/1/17 16:24
 */
public class demo1 {
	public static void main(String[] args) {
		ArrayList<Integer> integers = Lists.newArrayList(1, 2, 3, 4, 5, 6, 7, 8);
		List<Integer> collect = integers.stream().sorted().limit(Integer.MAX_VALUE).collect(Collectors.toList());
		System.out.println(collect);
	}
}
