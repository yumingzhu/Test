package May;

import June.AudiPartitioner;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * @Description TODO
 * @Author yumigzhu
 * @Date 2019/5/17 10:13
 */
public class KafkaJavaDemo {
	public static void main(String[] args) throws Exception {
		testproducer();
		//		receive();
	}

	public static String receive() {
		Properties props = new Properties();
		props.put("bootstrap.servers", "192.168.1.10:9092,192.168.1.11:9092,192.168.1.12:9092");
		props.put("topic", "test1");
		props.put("group.id", "test");
		props.put("enable.auto.commit", "true");
		props.put("auto.commit.interval.ms", "1000");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
		consumer.subscribe(Arrays.asList(props.getProperty("topic")));

		while (true) {
			ConsumerRecords<String, String> records = consumer.poll(100);
			for (ConsumerRecord<String, String> record : records) {
				System.out.println(record.toString());
			}
		}
	}

	//
	public static void testproducer() throws Exception {
		Properties props = new Properties();
		//		props.put("bootstrap.servers", "192.168.2.52:9092");
		props.put("bootstrap.servers", "192.168.1.30:9092,192.168.1.31:9092,192.168.1.32:9092");

		props.put("acks", "0");
		//        props.put("retries", 0);
		//        props.put("batch.size", 16384);
		//        props.put("linger.ms", 1);
		//        props.put("buffer.memory", 33554432);
		//		props.put("patitioner.class","June.AudiPartitioner");
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		Producer<String, String> producer = new KafkaProducer<>(props);
		String teststr = "[2019-05-16 00:09:03] INFO {\"id\": \"10929E3D00005CDC399F060A3E\",\"at\": 2,\"site\": {\"name\": \"\", \"3\": [\"\"], \"4\": [\"\"]},\"device\": {\"ip\": \"61.158.146.16\",\"ua\": \"\",\"openudid\": \"3744ce7ad8830bea989a56008b323e08\",\"carrier\": 46009,\"os\": \"Android 7.1.1\",\"connection_type\": 6,\"mac\": \"E3F5536A141811DB40EFD6400F1D0A4E\",\"imei\": \"01EE8D8F53178BE858DAAFCA94D8E084\",\"androidid\": \"4DAB4316142FB34EDC0B9B3D665609E3\", \"8\": [\"OPPO\"], \"9\": [\"OPPO R11t\"], \"11\": [\"Android 7.1.1\"], \"14\": [4]},\"user\": {\"id\": \"AQEBQcljc30tv9DOj6G3PApaCz1wIsRumt4v\",\"buyerid\": \"\"},\"impression\": [{\"id\": \"10929E3D00005CDC399F060A3E32\",\"tagid\": \"LV_1001_YDLDVi_LD\",\"bidfloor\": 0.0,\"video\": {\"mimes\": [\"mp4\",\"flv\"],\"minduration\": 0,\"maxduration\": 15000,\"width\": 640,\"height\": 360, \"2\": [1], \"5\": [1]},\"dealid\": \"5853170\",\"adm_require\": [{\"width\": 640,\"height\": 360,\"mimes\": \"mp4,flv\"}],\"channel\": \"4003\",\"display_type\": [{\"type\": 5,\"click_type\": [0]}], \"11\": [\"\\b\\b\"], \"14\": [3]}],\"app\": {\"name\": \"腾讯视频 6.9.5.18490\", \"1\": [\"\"], \"3\": [\"lives.l.qq.com\"]},\"is_order_cache\": false,\"is_first_time\": false}";
		producer.send(new ProducerRecord<String, String>("tencent", teststr));
		producer.close();

	}

	//
	public static void testproducerPatitioner() throws Exception {
		Properties props = new Properties();
		props.put("bootstrap.servers", "192.168.2.52:9092");
		props.put("acks", "0");
		//        props.put("retries", 0);
		//        props.put("batch.size", 16384);
		//        props.put("linger.ms", 1);
		//        props.put("buffer.memory", 33554432);
		//		props.put("patitioner.class","June.AudiPartitioner");
		props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, AudiPartitioner.class.getCanonicalName());//自定义分区函数
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		Producer<String, String> producer = new KafkaProducer<>(props);
		for (int i = 0; i < 10; i++) {
			//			Thread.sleep(10000);
			producer.send(new ProducerRecord<String, String>("test1", Integer.toString(i), Integer.toString(i)));
		}
		producer.close();

	}
}
