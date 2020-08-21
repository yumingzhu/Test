package Dec.flink;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.DataSetUtils;

/**
 * @Description TODO
 * @Author yumigzhu
 * @Date 2019/1/24 9:26
 */
public class FlinkTest2 {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();
        env.setParallelism(2);
        DataSource<String> in = env.fromElements("A", "B", "C", "D", "E", "F", "G", "H");
        DataSet<Tuple2<Long, String>> result = DataSetUtils.zipWithIndex(in);
//        result.writeAsText("G:\\test\\xxx.txt");
//        env.execute();
//        DataSet<Tuple2<Long, String>> result2 = DataSetUtils.zipWithUniqueId(in);
//        result2.writeAsText("G:\\test\\xxx2.txt");
//        env.execute();
        env.readTextFile("hdfs://master:8020/jiawei/result4").print();



    }
}
