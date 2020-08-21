package May;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * @Description TODO
 * @Author yumigzhu
 * @Date 2019/5/21 10:42
 */
public class IqiyiTest {
	public static void main(String[] args) {
		Properties props = new Properties();
		props.put("bootstrap.servers", "192.168.1.30:9092,192.168.1.31:9092,192.168.1.32:9092");
		props.put("acks", "1");

		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		Producer<String, String> producer = new KafkaProducer<>(props);

		String topic = "yiche";
		sendLine("G:\\test\\yiche", producer, topic);
	}

	public static void testproducer() throws Exception {
		Properties props = new Properties();
		props.put("bootstrap.servers", "192.168.1.10:9092,192.168.1.11:9092,192.168.1.12:9092");
		props.put("acks", "0");

		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		Producer<String, String> producer = new KafkaProducer<>(props);
		for (int i = 0; i < 10; i++) {
			//			Thread.sleep(10000);
			producer.send(new ProducerRecord<String, String>("iqiyi", Integer.toString(i), Integer.toString(i)));
		}
		producer.close();

	}

	/**
	 * 以行为单位读取文件，常用于读面向行的格式化文件 
	 */
	public static void sendLine(String fileName, Producer<String, String> produncer, String topic) {
		File file = new File(fileName);
		BufferedReader reader = null;
		try {
			System.out.println("以行为单位读取文件内容，一次读一整行:");
			reader = new BufferedReader(new FileReader(file));
			String tempString = null;
			// 一次读入一行，直到读入null为文件结束
			while ((tempString = reader.readLine()) != null) {
				System.out.println("tempString = " + tempString);
				produncer.send(new ProducerRecord<String, String>(topic, tempString));
			}
			produncer.close();
			reader.close();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (reader != null) {
				try {
					reader.close();
				} catch (IOException e1) {
				}
			}
		}
	}

}
