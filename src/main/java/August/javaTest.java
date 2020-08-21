package August;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @Description TODO
 * @Author yumigzhu
 * @Date 2019/8/14 11:39
 */
public class javaTest {
	public static void main(String[] args) {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
		Date date = new Date();
		long l = date.getTime() - 1000;
		Date date2 = new Date(l);
		System.out.println(sdf.format(date));
		System.out.println(sdf.format(date2));
	}

	public static void quiksort(int n[], int left, int right) {

		int dp;

		if (left < right) {

			dp = partition(n, left, right);

			quiksort(n, left, dp - 1);

			quiksort(n, dp + 1, right);

		}

	}

	public static int partition(int n[], int left, int right) {

		int pivot = n[left];

		while (left < right) {

			while (left < right) {

				while (left < right && n[right] >= pivot)

					right--;

				if (left < right)
					n[left++] = n[right];

				while (left < right && n[left] <= pivot)
					left++;

				if (left < right)
					n[right--] = n[left];

			}

			n[left] = pivot;

			return left;

		}
		return 0;
	}

}
