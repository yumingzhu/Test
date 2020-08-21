package June;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.PartitionInfo;

import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * @Description 自定义分区
 * @Author yumigzhu
 * @Date 2019/6/28 17:39
 */
public class AudiPartitioner implements Partitioner {

	private Random random;

	@Override
	public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
		List<PartitionInfo> partitionInfos = cluster.availablePartitionsForTopic("test1");
		int size = partitionInfos.size();
		String keyString = key.toString();

		return 0;
	}

	@Override
	public void close() {

	}

	@Override
	public void configure(Map<String, ?> map) {
		random = new Random();
	}
}
