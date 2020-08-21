//package Dec.flink
//
//import java.util.Properties
//
//import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
//import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
//import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer08
//import org.apache.flink.streaming.util.serialization.SimpleStringSchema
//
//object FlinkStream {
//  private val ZOOKEEPER_HOST = "127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183"
//  private val KAFKA_BROKER = "127.0.0.1:9092,127.0.0.1:9093,127.0.0.1:9094"
//  private val TRANSACTION_GROUP = "test"
//
//  def main(args : Array[String]){
//    val env = StreamExecutionEnvironment.getExecutionEnvironment
//    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
//    env.enableCheckpointing(1000)
//    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
//
//    // configure Kafka consumer
//    val kafkaProps = new Properties()
//    kafkaProps.setProperty("zookeeper.connect", ZOOKEEPER_HOST)
//    kafkaProps.setProperty("bootstrap.servers", KAFKA_BROKER)
//    kafkaProps.setProperty("group.id", TRANSACTION_GROUP)
//
//    //topicd的名字是new，schema默认使用SimpleStringSchema()即可
//    val transaction = env
//      .addSource(new FlinkKafkaConsumer08[String]("new", new SimpleStringSchema(), kafkaProps))
//    transaction.print()
//    env.execute()
//
//  }
//}
