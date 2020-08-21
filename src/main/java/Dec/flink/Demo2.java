package Dec.flink;

import java.util.List;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.ReduceOperator;
import org.apache.flink.util.Collector;

/**
 * @Description TODO
 * @Author yumigzhu
 * @Date 2019/1/8 14:28
 */
public class Demo2 {
	public static void main(String[] args) {
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<String> text = env.readTextFile("G:\\test\\demo.txt");
		ReduceOperator<WordWithCount> counts = text.flatMap(new FlatMapFunction<String, WordWithCount>() {
			@Override
			public void flatMap(String s, Collector<WordWithCount> collector) throws Exception {
				String[] tokens = s.toLowerCase().split("\\s+");
				for (String token : tokens) {
					if (token.length() > 0) {
						collector.collect(new WordWithCount(token, 1));
					}
				}
			}
		}).groupBy("word")//直接指定字段名称
				.reduce(new ReduceFunction<WordWithCount>() {
					@Override
					public WordWithCount reduce(WordWithCount wc, WordWithCount t1) throws Exception {
						return new WordWithCount(wc.word, wc.count + t1.count);
					}
				});
		try {
			List<WordWithCount> list = counts.collect();
			for (WordWithCount wordWithCount : list) {
				System.out.println(wordWithCount);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	//pojo
	public static class WordWithCount {
		public String word;
		public long count;

		public WordWithCount() {
		}

		public WordWithCount(String word, long count) {
			this.word = word;
			this.count = count;
		}

		@Override
		public String toString() {
			return "WordWithCount{" + "word='" + word + '\'' + ", count=" + count + '}';
		}
	}
}
