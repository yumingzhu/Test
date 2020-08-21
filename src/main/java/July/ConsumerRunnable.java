package July;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

/**
 * @Description TODO
 * @Author yumigzhu
 * @Date 2019/7/2 19:20
 */
public class ConsumerRunnable implements Runnable {
	//每个线程维护私有的KafkaConsumer 实例
	private final KafkaConsumer<String, String> consumer;

	public ConsumerRunnable(String brokerList, String groupId, String topic) {
		Properties properties = new Properties();
        properties.put("bootstrap.servers", brokerList);
        properties.put("topic", topic);
        properties.put("group.id", groupId);
        properties.put("enable.auto.commit", "true");
        properties.put("auto.commit.interval.ms", "1000");
		properties.put("session.timeout.ms", "30000");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

		this.consumer = new KafkaConsumer<String, String>(properties);
		this.consumer.subscribe(Arrays.asList(topic));  // 本例使用分区副本自动分配策略

	}

	@Override
	public void run() {
		while (true) {
			ConsumerRecords<String, String> records = consumer.poll(200);
			for (ConsumerRecord<String, String> record : records) {
				String name = Thread.currentThread().getName();
				System.out.println(name + "\t" + record.value() + "\t" + record.offset());
			}
		}
	}
}
