package Apr;

/**
 * @Description TODO
 * @Author yumigzhu
 * @Date 2019/4/2 11:48
 */

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
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

public class TestStreaming2 {
	private static final Pattern SPACE = Pattern.compile(" ");
	private static String CHECKPOINT_DIR = "G:/test/checkpoint2";
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
		jssc.checkpoint(CHECKPOINT_DIR);
		Set<String> topicsSet = new HashSet<>(Arrays.asList(topics.split(",")));
		Map<String, String> kafkaParams = new HashMap<>();
		kafkaParams.put("metadata.broker.list", brokers);
		kafkaParams.put("group.id", "group1");
		kafkaParams.put("auto.offset.reset", "smallest");

		Map<TopicAndPartition, Long> maptopic = new HashMap<>();
		for (int i = 0; i < 3; i++) {
			TopicAndPartition topic = new TopicAndPartition("test1", i);
			Long longValue = 1L;
			maptopic.put(topic,longValue);
		}

		//		// Create direct kafka stream with brokers and topics
		//		JavaPairInputDStream<String, String> directKafkaStream = KafkaUtils.createDirectStream(jssc, String.class, String.class,
		//				StringDecoder.class, StringDecoder.class, kafkaParams, topicsSet);

		// create direct stream

		JavaInputDStream<String> message = KafkaUtils.createDirectStream(jssc, String.class, String.class,
				StringDecoder.class, StringDecoder.class, String.class, kafkaParams, maptopic,
				new Function<MessageAndMetadata<String, String>, String>() {
					public String call(MessageAndMetadata<String, String> v1) throws Exception {
						return v1.message();
					}
				});

		// 得到rdd各个分区对应的offset, 并保存在offsetRanges中
		final AtomicReference<OffsetRange[]> offsetRanges = new AtomicReference<OffsetRange[]>();

		JavaDStream<String> Dstream = message.transform(new Function2<JavaRDD<String>, Time, JavaRDD<String>>() {
			@Override
			public JavaRDD<String> call(JavaRDD<String> v1, Time v2) throws Exception {
				OffsetRange[] offsets = ((HasOffsetRanges) v1.rdd()).offsetRanges();
				offsetRanges.set(offsets);
				return v1;
			}
		});
		JavaPairDStream<String, Integer> pairs = Dstream
				.flatMapToPair(new PairFlatMapFunction<String, String, Integer>() {
					@Override
					public Iterator<Tuple2<String, Integer>> call(String s) throws Exception {
						List<Tuple2<String, Integer>> list = new ArrayList<>();
						String[] split = s.split(" ");

						for (String string : split) {
							list.add(new Tuple2<>(string, 1));
						}
						return list.iterator();
					}
				});

		pairs.updateStateByKey(new Function2<List<Integer>, Optional<Integer>, Optional<Integer>>() {
			@Override
			public Optional<Integer> call(List<Integer> values, Optional<Integer> state) throws Exception {
				// 定义一个全局的计数
				Integer newValue = 0;
				if (state.isPresent()) {
					newValue = state.get();
				}
				for (Integer value : values) {
					newValue += value;
				}
				return Optional.of(newValue);
			}
		}).print();


		// Start the computation
		jssc.start();
		jssc.awaitTermination();
		jssc.close();
	}
}
