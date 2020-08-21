package Apr.jvmDemo;

import java.util.ArrayList;
import java.util.List;

/**
 * @Description TODO
 * @Author yumigzhu
 * @Date 2020/4/9 16:58
 */
public class TestOOM {
	public static void main(String[] args) throws InterruptedException {
		List<byte[]> list = new ArrayList<>();
		while (true) {
			byte[] e = new byte[1024 * 1024 * 10];
			Thread.sleep(1000);
			list.add(e);
		}

	}

}
