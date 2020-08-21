package Dec.flink

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.time.Time

object FinkCount {
  // the port to connect to
  def main(args: Array[String]): Unit = {
//    // get the execution environment
//    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
//
//    // get input data by connecting to the socket
//    val text = env.socketTextStream("localhost", 9999, '\n')
//
//    // parse the data, group it, window it, and aggregate the counts
//    val windowCounts = text
//
//      .map { w => WordWithCount(w, 1) }
//      .keyBy("word")
//      .timeWindow(Time.seconds(5), Time.seconds(1))
//      .sum("count")
//
//    // print the results with a single thread, rather than in parallel
//    windowCounts.print().setParallelism(1)
//
//    env.execute("Socket Window WordCount")
  }


// Data type for words with count
case class WordWithCount(word: String, count: Long)
}
