package Dec.flink;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;


import java.util.List;

/**
 * @Description TODO
 * @Author yumingzhu
 * @Date 2019/1/8 11:50
 */
public class Demo1 {
    public static void main(String[] args) {
        //获取执行环境 ExecutionEnvironment  (批处理对象)
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //加载数据源到 DataSet
        DataSet<String> text = env.readTextFile("G:\\test\\demo.txt");
        AggregateOperator<Tuple2<String, Integer>> counts = text.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
                String[] tokens = s.toLowerCase().split("\\s+");
                for (String token : tokens) {
                    if (token.length() > 0) {
                        // 初始化每一个单词， 保存为元组对象
                        collector.collect(new Tuple2<String, Integer>(token, 1));
                    }
                }

            }
        }).groupBy(0) // 0表示Tupel2<String ,Integer> 中的第一个元素， 及根据单词进行分组
        .aggregate(Aggregations.SUM, 1);// 同理， 1代表 第二个元素出现的次数， 进行求和
        try {
            List<Tuple2<String, Integer>> list = counts.collect();
            for (Tuple2<String, Integer> tuple2 : list) {
                System.out.println(tuple2.f0+":" +tuple2.f1);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }



}
