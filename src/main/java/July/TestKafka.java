package July;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.requests.CreateTopicsRequest;

import java.io.IOException;
import java.util.*;

/**
 * @Description 测试kafka 案例
 * @Author yumigzhu
 * @Date 2019/7/1 9:10
 */
public class TestKafka {
	public static void main(String[] args) {
		testKafkaInterceptor();
	}

	public static void testKafkaInterceptor() {
		Properties props = new Properties();
		props.put("bootstrap.servers", "192.168.2.52:9092");
		props.put("acks", "0");
		//        props.put("retries", 0);
		//        props.put("batch.size", 16384);
		//        props.put("linger.ms", 1);
		//        props.put("buffer.memory", 33554432);
		List<String> interceptor = new ArrayList<>();
		interceptor.add("July.TimeStampPrependderInterceptor");
		interceptor.add("July.CounterInterceptor");
		props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, interceptor);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		Producer<String, String> producer = new KafkaProducer<>(props);
		for (int i = 0; i < 10; i++) {
			//			Thread.sleep(10000);
			producer.send(new ProducerRecord<String, String>("test1", Integer.toString(i), Integer.toString(i)));
		}
		producer.close();
	}

	public void createTopics(String topicName, int patitions, short replicationFator) throws IOException {
		Map<String, CreateTopicsRequest.TopicDetails> topic = new HashMap<>();
		//插入多个有元素便可同时创建多个topic
		topic.put(topicName, new CreateTopicsRequest.TopicDetails(patitions, replicationFator));
		int creationTimeoutMs = 60000;
		CreateTopicsRequest request = new CreateTopicsRequest.Builder(topic, creationTimeoutMs).build();
	}

}
