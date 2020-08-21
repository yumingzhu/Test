import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.Map;

import static org.elasticsearch.spark.rdd.api.java.JavaEsSpark.esRDD;

/**
 * @Description TODO
 * @Author yumigzhu
 * @Date 2019/2/28 10:08
 */
public class TestSparkES {
	public static void main(String[] args) {
		SparkConf sparkConf = new SparkConf().setAppName("writeEs").setMaster("local")
				.set("es.index.auto.create", "true").set("es.nodes", "192.168.1.7,192.168.1.8,192.168.1.9")
				.set("es.port", "9200").set("es.nodes.wan.only", "true");
		SparkSession sparkSession = SparkSession.builder().config(sparkConf).getOrCreate();
		JavaSparkContext jsc = new JavaSparkContext(sparkSession.sparkContext());//adapter
		String query="\"query\":{\"bool\":{\"must\":[{\"match\":{\"country\":{\"query\":\"中国\",\"type\":\"phrase\"}}}]}}";
		JavaPairRDD<String, Map<String, Object>> accountsRDD = esRDD(jsc, "aes_tag_data","");

		accountsRDD.foreach(new VoidFunction<Tuple2<String, Map<String, Object>>>() {
			@Override
			public void call(Tuple2<String, Map<String, Object>> stringMapTuple2) throws Exception {
				String str = stringMapTuple2._1;
				Map<String, Object> stringMapTuple21 = stringMapTuple2._2;
				for (Map.Entry<String, Object> key : stringMapTuple21.entrySet()) {
					System.out.println(key + "\t" + stringMapTuple21.get(key));
				}
			}
		});

	}

}
