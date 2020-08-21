package Apr;

/**
 * @Description TODO
 * @Author yumigzhu
 * @Date 2019/4/2 11:48
 */
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;

import Utils.RedisUtil;
import kafka.common.TopicAndPartition;
import kafka.message.MessageAndMetadata;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.Optional;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.kafka.HasOffsetRanges;
import org.apache.spark.streaming.kafka.OffsetRange;
import redis.clients.jedis.Jedis;
import scala.Tuple2;

import kafka.serializer.StringDecoder;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.api.java.*;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.streaming.Durations;

public class TestStreaming {
	private static final Pattern SPACE = Pattern.compile(" ");

	public static void main(String[] args) throws InterruptedException {
		args = new String[] { "localhost:9092", "test1" };
		if (args.length < 2) {
			System.err.println("Usage: JavaDirectKafkaWordCount <brokers> <topics>\n"
					+ "  <brokers> is a list of one or more Kafka brokers\n"
					+ "  <topics> is a list of one or more kafka topics to consume from\n\n");
			System.exit(1);
		}

		String brokers = args[0];
		String topics = args[1];

		// Create context with a 2 seconds batch interval
		SparkConf sparkConf = new SparkConf().setMaster("local[2]").setAppName("JavaDirectKafkaWordCount");
		JavaStreamingContext jssc = new JavaStreamingContext(sparkConf, Durations.seconds(10));
		Set<String> topicsSet = new HashSet<>(Arrays.asList(topics.split(",")));
		Map<String, String> kafkaParams = new HashMap<>();
		kafkaParams.put("metadata.broker.list", brokers);
		kafkaParams.put("group.id", "group1");
		kafkaParams.put("auto.offset.reset", "smallest");

		Map<TopicAndPartition, Long> maptopic = new HashMap<>();
		Jedis jedis = RedisUtil.getJedis();
		Boolean flag = jedis.exists("offset");
		//判断redis 中是否有保存的偏移量， 如果有 直接读取redis 中的，没有的话 从头开始读取
		if (flag) {
			Map<String, String> offsets = jedis.hgetAll("offset");
			for (Map.Entry<String, String> entry : offsets.entrySet()) {
				System.out.println(entry.getKey() + "\t" + entry.getValue());
				TopicAndPartition topicP = new TopicAndPartition("test1", Integer.valueOf(entry.getKey()));
				maptopic.put(topicP, Long.valueOf(entry.getValue()));
			}

		}

		//		// Create direct kafka stream with brokers and topics
		//		JavaPairInputDStream<String, String> directKafkaStream = KafkaUtils.createDirectStream(jssc, String.class, String.class,
		//				StringDecoder.class, StringDecoder.class, kafkaParams, topicsSet);

		// create direct stream
		//参数说明， jssc ,  kafka中记录的key 类型， kafka 中记录的value类型， key 的解码方式，value 的解码方式，
		 // kafka 中记录的类型， 为kafka 设置的参数，    offsets 的集合 (这个会单独拿出来介绍的)，
		JavaInputDStream<String> message = KafkaUtils.createDirectStream(jssc, String.class, String.class,
				StringDecoder.class, StringDecoder.class, String.class, kafkaParams, maptopic,
				new Function<MessageAndMetadata<String, String>, String>() {
					public String call(MessageAndMetadata<String, String> v1) throws Exception {
						return v1.message();
					}
				});

		message.foreachRDD(new VoidFunction<JavaRDD<String>>() {
			@Override
			public void call(JavaRDD<String> stringJavaRDD) throws Exception {
				// 获取jedis 连接对象 ，
				Jedis jedis = RedisUtil.getJedis();
				OffsetRange[] offsetRanges = ((HasOffsetRanges) stringJavaRDD.rdd()).offsetRanges();
				//每次操作之前  ，保存此次操作前的偏移量， 如果当前任务失败， 我们可以回到开始的偏移量 重新计算，
				for (OffsetRange o : offsetRanges) {
					System.out.println(o.topic() + "\t" + o.partition() + "\t" + o.fromOffset() + "\t" + o.untilOffset());
					jedis.hset("offset", o.partition() + "", o.fromOffset() + "");
				}
				//计算过程

				stringJavaRDD.flatMapToPair(new PairFlatMapFunction<String, String, Integer>() {
					@Override
					public Iterator<Tuple2<String, Integer>> call(String s) throws Exception {
						List<Tuple2<String, Integer>> list = new ArrayList<>();
						String[] split = s.split(" ");

						for (String string : split) {
							list.add(new Tuple2<>(string, 1));
						}
						return list.iterator();
					}
				}).reduceByKey(new Function2<Integer, Integer, Integer>() {
					@Override
					public Integer call(Integer v1, Integer v2) throws Exception {
						return v1 + v2;
					}
				});
			}
		});

		// Start the computation
		jssc.start();
		jssc.awaitTermination();
		jssc.close();
	}
}
