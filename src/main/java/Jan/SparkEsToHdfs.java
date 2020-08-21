package Jan;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;

import java.util.HashMap;
import java.util.Map;

/**
 * @Description TODO
 * @Author yumigzhu
 * @Date 2019/3/5 11:08
 */
public class SparkEsToHdfs {
	public static void main(String[] args) {
//		long startTime = System.currentTimeMillis();
//		SparkConf sparkConf = new SparkConf()
//				                .setAppName("SparkEsToHdfs")
//				.set("es.nodes", "192.168.1.7,192.168.1.8").set("es.port", "9200")
//				//                .set("es.read.metadata", "true")
//				.set("es.nodes.wan.only", "true");
//        Map<String,String> options = new HashMap<>();("path" -> "spark/index",
//                "pushdown" -> "true",
//                "es.nodes" -> "someNode", "es.port" -> "9200")
//		SparkSession spark = SparkSession.builder().config(sparkConf).getOrCreate();
//		JavaSparkContext javaSparkContext = JavaSparkContext.fromSparkContext(spark.sparkContext());
//        String queryStr="{\"query\":{\"term\":{\"name\":\"hello123\"}}}";
//        JavaPairRDD<String, String> esPairRDD = JavaEsSpark.esJsonRDD(javaSparkContext, "aes_tag_data", queryStr);
//

	}
}
