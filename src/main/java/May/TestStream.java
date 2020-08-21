package May;

import org.apache.calcite.rel.core.Collect;

import java.awt.*;
import java.util.*;
import java.util.List;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**  关于 Stream 的创建的操作, 和  intermediate  操作
 * @Description TODO
 * @Author yumigzhu
 * @Date 2019/5/10 15:23
 */
public class TestStream {
	public static void main(String[] args) {
		Map<Boolean, Long> collect = Stream.of(1, 2, 3, 4, 5).collect(Collectors.partitioningBy(it -> it.intValue() % 2 == 0, Collectors.counting()));
		System.out.println(collect);

	}

	private static void joining_demo() {
		String strJoin =Stream.of("1","2","3","4","5").collect(Collectors.joining(",","[","]"));
		System.out.println("strJoin = " + strJoin);
	}

	private static void groupBy_demo() {
		Map<Boolean, List<Integer>> collect = Stream.of(1, 2, 3, 4).collect(Collectors.groupingBy(it -> it > 3));
		System.out.println(collect);
	}
	//根据  key 进行分组
	private static void partitioningBy_demo() {
		Map<Boolean, List<Integer>> collect = Stream.of(1, 2, 3, 4, 5).collect(Collectors.partitioningBy(it -> it % 2 == 0));
		System.out.println(collect);
	}

	//如果全部不满住 返回true
	private static void noneMatch_demo() {
		boolean b = Stream.of(1, 2, 3, 4).noneMatch(x -> x > 5);
		System.out.println("b = " + b);
	}

	private static void limit_demo() {
		Stream.of(1,2,3,4,5,6).limit(3).forEach(System.out::println);
	}

	private static void findAny_demo() {
		Optional<Integer> any = Stream.of(1, 2, 3, 4, 5).findAny();
		System.out.println(any.get());
	}

	private static void anyMatch_demo() {
		boolean anyMatch = Stream.of(1, 2, 3, 4).anyMatch(integer -> integer >3);
		System.out.println(anyMatch);
	}

	// 判断是否全满足条件， 如果全满足条件返回true
	private static void allMatch_demo() {
		boolean allMatch = Stream.of(1, 2, 3, 4).allMatch(integer -> integer > 0);
		System.out.println("allMatch" + allMatch);
	}

	private static void reduce_demo() {
		Optional<Integer> reduce = Stream.of(1, 2, 3, 4, 5).reduce((a, b) -> b = a + b);
		System.out.println(reduce.get());
	}

	private static void max_demo() {
		Optional<Integer> max = Stream.of(1, 2, 3, 4, 5).max((a, b) -> a - b);
		System.out.println(max.get());
	}

	//排序   倒叙
	private static void sorted__demo() {
		Stream.of(5, 4, 3, 2, 1).sorted(Comparator.reverseOrder()).forEach(System.out::println);
	}

	// 设置跳过元素的个数
	private static void skip__demo() {
		Stream.of(1, 2, 3, 4, 5).skip(2).forEach(System.out::println);
	}

	//peek 函数相当于 给下一函数 ，添加了一个   触发前的事件
	private static void peek__demo() {
		Stream.of(1, 2, 3, 4, 5, 6).peek(integer -> System.out.println("accept:" + integer)).map(x -> x * 10)
				.forEach(System.out::println);
	}

	private static void faltMap_demo() {
		Stream.of(1, 2, 3).flatMap(integer -> Stream.iterate(1, item -> item + 1).limit(integer))
				.forEach(System.out::println);
	}

	private static void map__Demo() {
		Stream.of("a", "b", "c").map(item -> item.toUpperCase()).forEach(System.out::println);
	}

	private static void filter__Demo() {
		Stream.of(1, 2, 3, 4, 5).filter(x -> x > 2).forEach(System.out::println);
	}

	//去重操作
	private static void distinct__Demo() {
		Stream.of(1, 2, 3, 4, 4).distinct().forEach(x -> System.out.println(x));
	}

	//将俩个 Stream 拼接在一起的操作
	private static void concat__Demo() {
		Stream.concat(Stream.of(1, 2, 3, 4), Stream.of(4, 5, 6, 7)).forEach(System.out::println);
	}

	private static void testSteam4() {
		int ids[] = new int[] { 1, 2, 3, 4 };
		Arrays.stream(ids).forEach(System.out::println);
	}

	private static void testStream3() {
		Stream.iterate(1, item -> item + 1).limit(5).forEach(System.out::println);
	}

	private static void testStream2() {
		//获取无限长度的 Stream
		Stream.generate(new Supplier<Double>() {
			@Override
			public Double get() {
				return Math.random();
			}
		});
		Stream<Double> generateC = Stream.generate(Math::random);
		generateC.forEach(x -> System.out.println(x));
	}

	private static void testStream1() {
		Stream<Integer> integerStream = Stream.of(1, 2, 3, 6, 4, 9, 7);
		List<Integer> collect = integerStream.sorted().collect(Collectors.toList());
	}

}
