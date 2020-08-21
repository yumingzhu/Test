package May;

import Dec.bean.Student;
import org.junit.Before;

import java.time.DayOfWeek;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;

/**
 * @Description TODO
 * @Author yumigzhu
 * @Date 2019/5/6 16:19
 */
public class Test {
	static  List<Student> stuList;


	public static void test1() {
		List<String> studentList = stuList.stream().filter(x -> x.getScore() > 85)
				.sorted(Comparator.comparing(Student::getScore).reversed()).map(Student::getName)
				.collect(Collectors.toList());
		System.out.println(studentList);



	}

	public static void main(String[] args) {
//		//         MyLamda m=y-> System.out.println("ss"+y);
//		//         m.test1("zz");
//		//         TestConverT<String,Integer>  t=Integer::valueOf;
//		//        Integer convert = t.convert("11");
//		//        System.out.println(convert);
//		LocalDateTime dt = LocalDateTime.now();
//		DayOfWeek dayOfWeek = dt.getDayOfWeek();
//		System.out.println(dt);
//		String str = "123456789";
        Random random = new Random();
        stuList = new ArrayList<Student>() {
            {
                for (int i = 0; i < 100; i++) {
                    add(new Student("student" + i, random.nextInt(50) + 50));
                }
            }
        };
        test1();
		System.out.println("ea373bfc16e4752a9f36ce078f81b8d14fe31ae9".length());
	}

	static class Student {
		private String name;
		private Integer score;

		public Student(String name, Integer score) {
			this.name = name;
			this.score = score;
		}

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		public Integer getScore() {
			return score;
		}

		public void setScore(Integer score) {
			this.score = score;
		}
	}

}
