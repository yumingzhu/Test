package Dec;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestSparkJob {

	private final static Logger logger = LoggerFactory.getLogger(TestSparkJob.class);

	public static void main(String[] args) {
		SparkSession sparkSession = SparkSession.builder().master("local[*]").getOrCreate();
		JavaSparkContext javaSparkContext = JavaSparkContext.fromSparkContext(sparkSession.sparkContext());
		JavaRDD<String> stringJavaRDD = javaSparkContext.textFile("G:\\test\\request.log.zip");
		final long count = stringJavaRDD.count();
	}
}
