import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * @Description spark读取phoenix视图数据，差不多56s
 * @Author zhangjw
 * @Date 2018/11/8 18:13
 */
public class SparkSqlReadPhoenix {

	public static void main(String[] args) {
		SparkSession sparkSession = SparkSession.builder().master("local").appName("sparkSqlReadPhoenix").getOrCreate();

		String sql = "(select * from \"aes_tag_data\" where \"updated_at\" > '20181010') as result";
		Dataset<Row> dataset = sparkSession.read().format("jdbc")
				.option("driver", "org.apache.phoenix.jdbc.PhoenixDriver")
				.option("url", "jdbc:phoenix:192.168.1.7:2181").option("dbtable", sql).load();

		dataset.registerTempTable("result");
		Dataset<Row> result = sparkSession.sql("select * from result");
		JavaRDD<Row> javaRDD = result.javaRDD();
		javaRDD.foreach(new VoidFunction<Row>() {
			@Override
			public void call(Row row) throws Exception {
				System.out.println(row.get(0));
			}
		});
		sparkSession.close();
	}
}
