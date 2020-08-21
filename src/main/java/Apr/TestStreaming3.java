package Apr;

/**
 * @Description TODO
 * @Author yumigzhu
 * @Date 2019/4/2 11:48
 */

import java.util.*;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.HasOffsetRanges;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.streaming.kafka.OffsetRange;

import Utils.RedisUtil;
import kafka.common.TopicAndPartition;
import kafka.message.MessageAndMetadata;
import kafka.serializer.StringDecoder;
import redis.clients.jedis.Jedis;
import scala.Tuple2;

public class TestStreaming3 {
	private static final Pattern SPACE = Pattern.compile(" ");

	public static void main(String[] args) throws InterruptedException {
//		args = new String[] { "192.168.1.10:9092,192.168.1.11:9092,192.168.1.12:9092", "cs" };
		args = new String[] { "192.168.2.52:9092", "test" };
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
		for (int i = 0; i < 1; i++) {
			TopicAndPartition topic = new TopicAndPartition("test", i);
			Long longValue = 1L;
			maptopic.put(topic,longValue);
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
				OffsetRange[] offsetRanges = ((HasOffsetRanges) stringJavaRDD.rdd()).offsetRanges();
				//每次操作之前  ，保存此次操作前的偏移量， 如果当前任务失败， 我们可以回到开始的偏移量 重新计算，
				for (OffsetRange o : offsetRanges) {
					System.out.println(o.topic() + "\t" + o.partition() + "\t" + o.fromOffset() + "\t" + o.untilOffset());
				}
				stringJavaRDD.foreach(new VoidFunction<String>() {
					@Override
					public void call(String s) throws Exception {
						System.out.println(s);
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
