package Jan;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * @Description TODO
 * @Author yumigzhu
 * @Date 2019/3/12 17:21
 */
public class TestSql {
    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder().master("local").appName("winPriceProbability").getOrCreate();
        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

        Dataset<Row> dataset = spark.sql("select 2/0");
        dataset.show();


    }
}
