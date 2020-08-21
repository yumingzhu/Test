package Dec;

import org.apache.flink.api.common.functions.FoldFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditEvent;
import org.apache.flink.streaming.connectors.wikiedits.WikipediaEditsSource;
import scala.Tuple2;

/**
 * @Description  flink 入门
 * @Author 余明珠
 * @Date 2019/1/3
 */
public class WikipediaAnalysis {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment see = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<WikipediaEditEvent> edits = see.addSource(new WikipediaEditsSource());

        KeyedStream<WikipediaEditEvent, String> keyedEdits = edits.keyBy(new KeySelector<WikipediaEditEvent, String>() {
            @Override
            public String getKey(WikipediaEditEvent wikipediaEditEvent) throws Exception {
                return wikipediaEditEvent.getUser();
            }
        });
        SingleOutputStreamOperator<Tuple2<String, Long>> result = keyedEdits.timeWindow(Time.seconds(5)).fold(new Tuple2<>("", 0L), new FoldFunction<WikipediaEditEvent, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> fold(Tuple2<String, Long> acc, WikipediaEditEvent event) throws Exception {
                Tuple2<String, Long> tuple2 = new Tuple2<>(event.getUser(), event.getByteDiff() + acc._2);
                return tuple2;
            }
        });


        result.print();
        see.execute();
    }
}
