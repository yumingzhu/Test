//package May
//
//import java.util
//import java.util.Properties
//
//import kafka.consumer.{Consumer, ConsumerConfig}
//import kafka.producer.{KeyedMessage, Producer}
//import kafka.tools.ConsoleProducer.ProducerConfig
//import org.apache.kafka.clients.producer.Producer
//
//object KafkaDemo {
//  def main(args: Array[String]): Unit = {
//    testProducer
//
//  }
//
//  def testProducer: Unit = {
//    val props = new Properties()
//    props.put("serializer.class", "kafka.serializer.StringEncoder");
//    props.put("metadata.broker.list", "192.168.2.52:9092")
//    props.put("request.required.acks", "1")
//    val config = new ProducerConfig(props)
//    val producer = new Producer[String, String](config)
//    (1 to 100).foreach((i: Int) => {
//      print(".")
//      val msg = new KeyedMessage("test", "key", "msg" + i)
//      producer.send(msg)
//      Thread.sleep(10000)
//    })
//    producer.close()
//  }
//
//  def testConsumer: Unit = {
//    val topic = "test"
//    val props = new Properties()
//    props.put("zookeeper.connect", "192.168.2.52:2181")
//    props.put("group.id", "testGroup1")
//    val consumerConnector = Consumer.createJavaConsumerConnector(new ConsumerConfig(props))
//    val topicCountMap = new util.HashMap[String, Integer]()
//    topicCountMap.put(topic, 1) // TOPIC在创建时就指定了它有3个partition
//    val messageStreams = consumerConnector.createMessageStreams(topicCountMap)
//    val stream = messageStreams.get(topic).get(0)
//    val iterator = stream.iterator()
//    while (iterator.hasNext) {
//      print(".")
//      val msg = new String(iterator.next.message)
//      println(msg)
//    }
//
//
//  }
//
//}
