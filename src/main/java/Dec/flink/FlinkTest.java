package Dec.flink;

import Dec.bean.Order;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.functions.FormattingMapper;
import org.apache.flink.api.java.io.jdbc.JDBCInputFormat;
import org.apache.flink.api.java.operators.*;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.util.List;

/**
 * @Description TODO
 * @Author yumigzhu
 * @Date 2019/1/23 9:51
 */
public class FlinkTest {
	public static void main(String[] args) throws Exception {
		ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //设置执行重试次数
		env.setNumberOfExecutionRetries(3);

		DataSource<String> test = env.readTextFile("G:\\test\\demo.txt");
		test.map(new RichMapFunction<String, String>() {
			private IntCounter numLines = new IntCounter();

			@Override
			public void open(Configuration parameters) throws Exception {
				super.open(parameters);
				getRuntimeContext().addAccumulator("num-lines", this.numLines);
			}

			@Override
			public String map(String s) throws Exception {
				this.numLines.add(1);
				return s;
			}

		}).print();
		//获取累加器，  我太聪明了
		JobExecutionResult jobResult = env.getLastJobExecutionResult();
		int accumulatorResult = jobResult.getAccumulatorResult("num-lines");
		System.out.println("accumulatorResult = " + accumulatorResult);

		FlatMapOperator<String, Tuple2<String, Integer>> word = test.flatMap(new MyFlatMapFunction());

		word.groupBy(0).aggregate(Aggregations.SUM, 1).print();

		word.reduce(new ReduceFunction<Tuple2<String, Integer>>() {
			@Override
			public Tuple2<String, Integer> reduce(Tuple2<String, Integer> stringIntegerTuple2,
					Tuple2<String, Integer> t1) throws Exception {
				int i = stringIntegerTuple2.f1 + t1.f1;
				return new Tuple2<>(t1.f0, i);
			}
		});

		DataSource<Tuple2<String, Integer>> test2 = env.fromElements(new Tuple2<String, Integer>("hello", 1),
				new Tuple2<String, Integer>("word", 2), new Tuple2<String, Integer>("nihao", 3),
				new Tuple2<String, Integer>("hello", 3));

		DataSource<Tuple2<String, Integer>> test3 = env.fromElements(new Tuple2<String, Integer>("hello", 3),
				new Tuple2<String, Integer>("word", 4));
		JoinOperator.DefaultJoin<Tuple2<String, Integer>, Tuple2<String, Integer>> tuple2Tuple2DefaultJoin = test2
				.join(test3).where(0).equalTo(0);
		//join 操作
		JoinOperator<Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple2<String, Integer>> with = test2
				.leftOuterJoin(test3).where(0).equalTo(0)
				.with(new JoinFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple2<String, Integer>>() {
					@Override
					public Tuple2<String, Integer> join(Tuple2<String, Integer> v1, Tuple2<String, Integer> v2)
							throws Exception {
						Integer num = v1.f1;
						if (v2 != null) {
							num = v1.f1 + v2.f1;
						}
						return new Tuple2<>(v1.f0, num);
					}
				});
		with.print();
		test2.map(new MapFunction<Tuple2<String, Integer>, Integer>() {
			@Override
			public Integer map(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
				return stringIntegerTuple2.f1;
			}
		});
		//求笛卡尔积
		DataSource<Integer> data1 = env.fromElements(new Integer[] { 1, 2, 3, 4 });
		DataSource<Integer> data2 = env.fromElements(new Integer[] { 1, 2, 3, 5 });
		CrossOperator.DefaultCross<Integer, Integer> cross = data1.cross(data2);
		//cross.print();
		//将两个数据进行union 操作
		UnionOperator<Integer> union = data1.union(data2);
		union.print();
		//对数据进行均匀分区
		// data1.rebalance()
		//根据哈希进行分区
		//data1.partitionByHash(0);
		//取前n
		GroupReduceOperator<Integer, Integer> first = data1.first(2);
		first.print();
		GroupReduceOperator<Tuple2<String, Integer>, Tuple2<String, Integer>> first1 = test2.groupBy(0).first(1);
		first1.print();
		//从tuple  中获取 任何元素， 可以进行反转操作
		ProjectOperator<?, Tuple> project = test2.project(1, 0);
		project.print();
		System.out.println("获取当前元祖 中的最小值");
		//         test2. maxBy(1).print();
		test2.groupBy(0).minBy(1);

		//读取 csv 文件
		//        DataSource<Tuple2<String, String>> csvData = env.readCsvFile("E:\\order_winRDD.csv").types(String.class, String.class);
		//        csvData.print();
		//读取csv 文件  将文件映射成pojo
		//        env.readCsvFile("E:\\order_winRDD.csv").pojoType(Order.class,"orderid","name").print();

		//读取mysql 数据
		DataSource<Row> input = env.createInput(JDBCInputFormat.buildJDBCInputFormat()
				.setDrivername("com.mysql.jdbc.Driver").setDBUrl("jdbc:mysql://localhost:3306/test").setUsername("root")
				.setPassword("yumingzhu").setQuery("select id,name, age from user")
				.setRowTypeInfo(new RowTypeInfo(BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.STRING_TYPE_INFO,
						BasicTypeInfo.INT_TYPE_INFO))
				.finish());
		//		input.print();

		test2.sortPartition(1, org.apache.flink.api.common.operators.Order.DESCENDING);
		// 根据元祖进行整体的排序
		//		test2.sortPartition("*", org.apache.flink.api.common.operators.Order.DESCENDING).writeAsText("G:\\test");
		IterativeDataSet<Integer> iterate = env.fromElements(0).iterate(1000);
		MapOperator<Integer, Integer> iteration = iterate.map(new MapFunction<Integer, Integer>() {
			@Override
			public Integer map(Integer i) throws Exception {
				double x = Math.random();
				double y = Math.random();

				return i + ((x * x + y * y < 1) ? 1 : 0);
			}
		});

		// Iteratively transform the IterativeDataSet
		final DataSet<Integer> count = iterate.closeWith(iteration);

		count.map(new MapFunction<Integer, Double>() {
			@Override
			public Double map(Integer count) throws Exception {
				System.out.println("count = " + count);
				return count / (double) 10000 * 4;
			}
		}).print();
		DataSource<String> data3 = env.fromElements("a", "b");
		data2.map(new RichMapFunction<Integer, String>() {
			List<String> broadCastdata3 = null;

			@Override
			public void open(Configuration parameters) throws Exception {
				super.open(parameters);
				broadCastdata3 = getRuntimeContext().getBroadcastVariable("broadCastdata3");
			}

			@Override
			public String map(Integer integer) throws Exception {
				return integer + " " + broadCastdata3.get(0);
			}
		}).withBroadcastSet(data3, "broadCastdata3").print();

		//通过自定义函数的方式传递参数
		data2.filter(new MyFilter(2));
		//通过 withParameters() 传递参数
		Configuration conf = new Configuration();
		conf.setInteger("limit", 2);
		data2.filter(new RichFilterFunction<Integer>() {
			private int limit;

			@Override
			public void open(Configuration parameters) throws Exception {
				limit = parameters.getInteger("limit", 0);
			}

			@Override
			public boolean filter(Integer value) throws Exception {
				return value > limit;
			}
		}).withParameters(conf).print();




	}

	private static class MyFilter implements FilterFunction<Integer> {
		private final int limit;

		public MyFilter(int limit) {
			this.limit = limit;
		}

		@Override
		public boolean filter(Integer value) throws Exception {
			return value > limit;
		}
	}

	private static class MyFlatMapFunction implements FlatMapFunction<String, Tuple2<String, Integer>> {

		@Override
		public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
			String[] split = s.split(" ");
			for (String s1 : split) {
				collector.collect(new Tuple2(s1, 1));
			}
		}
	}
}
