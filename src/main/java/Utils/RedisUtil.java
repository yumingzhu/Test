package Utils;

import java.util.Collections;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class RedisUtil {

	//服务器IP地址
	private static String ADDR = "127.0.0.1";

	//端口
	private static int PORT = 6379;
	//密码
	private static String AUTH = "123456";
	//连接实例的最大连接数
	private static int MAX_ACTIVE = 1024;

	//控制一个pool最多有多少个状态为idle(空闲的)的jedis实例，默认值也是8。
	private static int MAX_IDLE = 200;

	//等待可用连接的最大时间，单位毫秒，默认值为-1，表示永不超时。如果超过等待时间，则直接抛出JedisConnectionException
	private static int MAX_WAIT = 10000;

	//连接超时的时间
	private static int TIMEOUT = 10000;

	// 在borrow一个jedis实例时，是否提前进行validate操作；如果为true，则得到的jedis实例均是可用的；
	private static boolean TEST_ON_BORROW = true;

	private static JedisPool jedisPool = null;

	private static Logger logger = LoggerFactory.getLogger(RedisUtil.class);

	private static final Long ONE = 1L;

	private static Jedis jedis = null;
	/**
	 * 初始化Redis连接池
	 */

	static {

		try {

			JedisPoolConfig config = new JedisPoolConfig();
			config.setMaxTotal(MAX_ACTIVE);
			config.setMaxIdle(MAX_IDLE);
			config.setMaxWaitMillis(MAX_WAIT);
			config.setTestOnBorrow(TEST_ON_BORROW);
			jedisPool = new JedisPool(config, ADDR, PORT, TIMEOUT);

		} catch (Exception e) {

			e.printStackTrace();
		}

	}

	/**
	 * 获取Jedis实例
	 */

	public synchronized static Jedis getJedis() {

		try {

			if (jedisPool != null) {
				Jedis resource = jedisPool.getResource();
				return resource;
			} else {
				return null;
			}

		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}

	}

	/***
	 *
	 * 释放资源
	 */

	public static void returnResource(final Jedis jedis) {
		if (jedis != null) {
			jedisPool.returnResource(jedis);
		}

	}

	/**
	 * redis分布式锁加锁
	 * @param key
	 * @param requestId
	 * @param expireTime
	 * @return
	 */
	public static boolean lock(String key, String requestId, int expireTime) {
		String result = jedis.set(key, requestId, "NX", "PX", expireTime);
		if ("OK".equals(result)) {
			logger.info("redis分布式锁key:{}加锁{}成功", key, requestId);
			return true;
		}

		logger.info("redis分布式锁获key{}加锁{}失败", key, requestId);
		return false;
	}

	public static void init() {
		jedis = RedisUtil.getJedis();
	}

	/**
	 * redis分布式锁，解锁
	 * 用lua脚本执行解锁，保证判断和解锁是在一个事件中执行的
	 * @return
	 */
	public static boolean unLock(String key, String requestId) {
		String script = "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end";
		Object result = jedis.eval(script, Collections.singletonList(key), Collections.singletonList(requestId));
		if (ONE.equals(result)) {
			logger.info("redis分布式锁key{}解锁{}成功", key, requestId);
			return true;
		}

		logger.info("redis分布式锁key{}解锁{}失败", key, requestId);
		return false;
	}

	public static void main(String[] args) {
		//		Jedis jedis = RedisUtil.getJedis();
		//		jedis.hset("map","a","1");
		//		jedis.hset("map","b","1");
		RedisUtil.init();
		String requestId = UUID.randomUUID().toString();
		System.out.println("requestId" + requestId);
		boolean insightkey = RedisUtil.lock("insightkey", requestId, 120000);
		while (true) {
			try {
				Thread.sleep(30000);
				boolean insightkey2 = RedisUtil.lock("insightkey",  UUID.randomUUID().toString(), 120000);
				if (insightkey2) {
					System.out.println("获取锁成功");
					break;
				}
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		//		jedis.hincrBy("map", "a", 1);
		//		String hget = jedis.hget("map", "a");

		//		System.out.println(hget);

	}
}