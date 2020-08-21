import Jan.Student;
import Utils.RedisUtil;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import redis.clients.jedis.Jedis;

import java.util.*;

/**
 * @Description TODO
 * @Author yumigzhu
 * @Date 2019/2/20 14:19
 */
public class Test {
	public static void main(String[] args) {
		RedisUtil.init();
		RedisUtil.unLock("insightkey","334d416c-8440-4867-8215-99a1070eb022");
	}

	private static void subJson() {
		String str = "{\"id\": \"83c639cad996d310c4636a41f99befe6\",\"user\": {\"id\": \"65d1b61958253500761a040e2c7d512f\"},\"site\": {\"id\": 1,\"content\": {\"url\": \"www.iqiyi.com\",\"keyword\": [\"悬疑\",\"类型\",\"网剧\",\"惊悚\",\"宫廷\",\"古装\",\"奇幻\",\"剧情\",\"地区\",\"内地\",\"内地剧场\"],\"len\": 1528,\"album_id\": 222057201,\"channel_id\": 2}},\"device\": {\"ip\": \"36.98.62.232\",\"geo\": {\"country\": 86,\"metro\": 8613,\"city\": 861311, \"7\": [0x405cc4ec2480e8c9], \"8\": [0x404304f80dc33722]},\"connection_type\": 2,\"platform_id\": 33,\"android_id\": \"8e9e818c0b70683f\",\"model\": \"OPPO A59s\",\"os\": \"android\",\"os_version\": \"5.1\",\"app_version\": \"9.11.5\"},\"imp\": [{\"id\": \"0_pmp\",\"video\": {\"ad_zone_id\": 1000000000381,\"linearity\": 1,\"minduration\": 15,\"maxduration\": 15,\"protocol\": 3,\"ad_type\": 1,\"is_entire_roll\": false},\"bidfloor\": 400,\"campaign_id\": 91141472,\"is_pmp\": true,\"floor_price\": [{\"industry\": 400000000,\"price\": 800},{\"industry\": 200000000,\"price\": 400},{\"industry\": 100000000,\"price\": 600},{\"industry\": 600000000,\"price\": 400},{\"industry\": 700000000,\"price\": 400},{\"industry\": 800000000,\"price\": 400},{\"industry\": 900000000,\"price\": 400},{\"industry\": 300000000,\"price\": 400}]}]}";
		int i = str.indexOf("\"7\"");
		String start = str.substring(0, i - 1);
		String substring = str.substring(i, str.length());
		int index = substring.indexOf("}");
		String end = substring.substring(index, substring.length());
		String string = start + end;

		JSONObject jsonObject = JSONObject.parseObject(string);
		System.out.println("jsonObject = " + jsonObject);
	}

	public static void testLamda1() {
		List<String> list = Arrays.asList("aaa", "bbb", "ccc");
		//支持了匿名函数
		Collections.sort(list, (a, b) -> b.compareTo(a));
		for (String string : list) {
			System.out.println(string);
		}

	}

	private static void testBitSet() {
		int[] array = new int[] { 1, 2, 3, 22, 0, 3 };
		BitSet bitSet = new BitSet(6);
		//将数组内容组bitmap
		for (int i = 0; i < array.length; i++) {
			bitSet.set(array[i], true);
		}
		System.out.println(bitSet.size());
		System.out.println(bitSet);
		System.out.println(bitSet.get(3));
		System.out.println(Integer.MAX_VALUE);
	}

	private static void testReids() {
		Jedis jedis = RedisUtil.getJedis();
//		jedis.hset("offset", 0 + "", 1 + "");
//		jedis.hset("offset", 1 + "", 1 + "");
//		jedis.hset("offset", 2 + "", 1 + "");
//		List<String> offset = jedis.hvals("offset");
//		jedis.sadd("black","192.168.2.132");
		System.out.println(jedis.sismember("black", "0xxx"));
		System.out.println(jedis.sismember("black", "192.168.2.132"));
	}

	private static void getTag_id() {
		String s = "I0000";
		int start = 2565;

		for (int i = 0; i < 395; i++) {
			String str = String.valueOf(i);
			int length = str.length();
			String s1 = s.substring(0, s.length() - length) + str;
			String sql = "UPDATE  hbase_tag  set  tag_id= '" + s1 + "'   where id=" + start + ";";
			start++;
			System.out.println(sql);
		}
	}

}
