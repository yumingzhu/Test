package Apr.testBlock;

import java.util.BitSet;

/**
 * @Description TODO
 * @Author yumigzhu
 * @Date 2019/5/5 10:18
 */
public class MainTestFive {
	public static void main(String[] args) {
		int[] shu = { 2, 42, 5, 6, 6, 18, 33, 15, 25, 31, 28, 37 };
		BitSet bm = new BitSet(MainTestFive.getMaxValue(shu));
		System.out.println("bm.size" + bm.size());
		MainTestFive.putValueIntoBitSet(shu, bm);
		printBitSet(bm);

	}

	//初始全部为false，这个你可以不用，因为默认都是false
	public static void initBitSet(BitSet bs) {
		for (int i = 0; i < bs.size(); i++) {
			bs.set(i, false);
		}
	}

	//打印
	public static void printBitSet(BitSet bs) {
		StringBuffer buf = new StringBuffer();
		buf.append("[\n");
		for (int i = 0; i < bs.size(); i++) {
			if (i < bs.size() - 1) {
				buf.append(MainTestFive.getBitTo10(bs.get(i)) + ",");
			} else {
				buf.append(MainTestFive.getBitTo10(bs.get(i)));
			}
			if ((i + 1) % 8 == 0 && i != 0) {
				buf.append("\n");
			}
		}
		buf.append("]");
		System.out.println(buf.toString());
	}

	//找出集合中的最大值
	private static int getMaxValue(int[] shu) {
		int temp = 0;
		temp = shu[0];
		for (int i = 0; i < shu.length; i++) {
			if (temp < shu[i]) {
				temp = shu[i];
			}
		}
		System.out.println("maxvalue" + temp);
		return 0;
	}

	//放值
	public static void putValueIntoBitSet(int[] shu, BitSet bs) {
		for (int i = 0; i < shu.length; i++) {
			bs.set(shu[i], true);
		}
	}

	//true false  换成  1，0，为 了 好看
	public static String getBitTo10(boolean flag) {
		String a = "";
		if (flag == true) {
			return "1";

		} else {
			return "0";
		}
	}

}
