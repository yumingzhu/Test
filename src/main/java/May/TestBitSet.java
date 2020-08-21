package May;

import java.util.BitSet;

/**
 * @Description TODO
 * @Author yumigzhu
 * @Date 2019/5/5 10:11
 */
public class TestBitSet {
	public static void main(String[] args) {


		BitSet bitSet = new BitSet();
		System.out.println(bitSet.isEmpty() + "--" + bitSet.size());
		bitSet.set(0);
        System.out.println(bitSet.isEmpty() + "--" + bitSet.size());
        bitSet.set(1);
        bitSet.set(65);
        System.out.println(bitSet.isEmpty() + "--" + bitSet.size());



    }
}
