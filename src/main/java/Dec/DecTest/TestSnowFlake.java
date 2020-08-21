package Dec.DecTest;

import javaTest.KeyWorker;

/**
 * @Description TODO
 * @Author yumigzhu
 * @Date 2019/1/28 11:31
 */
public class TestSnowFlake {

	public static void main(String[] args) {
        for (int i = 0; i <10 ; i++) {
            long id = KeyWorker.nextId();
            System.out.println("id = " + id);
        }
    }

}
