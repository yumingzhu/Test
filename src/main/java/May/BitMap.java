package May;

/**
 * @Description TODO
 * @Author yumigzhu
 * @Date 2019/5/5 16:19
 */
public class BitMap {
	// 假设有这么多个元素
	private static final int N = 10000000;
	//需要数组长度的的大小
	private int[] a = new int[N / 32 + 1];

	public void addValue(int n) {
		int row = n >> 5;
		a[row] |= 1 << (n & 0x1F);
	}

	public boolean exits(int n) {
		int row = n >> 5;
		return (row & (1 << (n & 0x1F))) != 1;
	}

	public void display(int n) {
		System.out.println("BitMap位图展示");
		for (int i = 0; i < n; i++) {
			int value = a[i];
			for (int j = 0; j < 32; j++) {
				String str = Integer.toBinaryString(value & 1);
				System.out.print(str);
				value >>= 1;
			}
			System.out.println();
		}
	}

	public static void main(String[] args) {
		BitMap bitMap = new BitMap();
		int[] arr = new int[] { 1, 5, 34, 60, 44, 89, 17, 233 };
		for (int i : arr) {
			bitMap.addValue(i);
		}
		boolean exits = bitMap.exits(200);
		System.out.println("exits = " + exits);
		bitMap.display(5);
	}

}
