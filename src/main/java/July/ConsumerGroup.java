package July;

import java.util.ArrayList;
import java.util.List;

/**
 * @Description 多个KafkaConsumer
 * @Author yumigzhu
 * @Date 2019/7/2 19:28
 */
public class ConsumerGroup {
	private List<ConsumerRunnable> consumers;

	public ConsumerGroup(int consumNum, String groupId, String topic, String brokerList) {
		consumers = new ArrayList<>(consumNum);
		for (int i = 0; i < consumNum; i++) {
			ConsumerRunnable consumerRunnable = new ConsumerRunnable(brokerList, groupId, topic);
			consumers.add(consumerRunnable);
		}
	}

	public void execute() {
		for (ConsumerRunnable consumer : consumers) {
			new Thread(consumer).start();
		}
	}

	public static void main(String[] args) {
		String brokerList = "192.168.2.52:9092";
		String groupId = "testGroup";
		String topic = "test1";
		int consumerNum = 2;
		ConsumerGroup consumerGroup = new ConsumerGroup(consumerNum, groupId, topic, brokerList);
		consumerGroup.execute();
	}
}
