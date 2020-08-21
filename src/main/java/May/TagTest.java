package May;

import May.bean.Tag;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.List;
import java.util.Properties;

/**
 * @Description TODO
 * @Author yumigzhu
 * @Date 2019/5/6 17:13
 */
public class TagTest {
    public static Properties getMysqlJDBCProperties() {
        Properties properties = new Properties();
        properties.put("user", "root");
        properties.put("password", "gnova2017!@#");
        properties.put("driver", "com.mysql.jdbc.Driver");
        return properties;
    }
    private static String JDBC_URL="jdbc:mysql://192.168.1.6:3306/eagle_log?useUnicode=true&characterEncoding=UTF-8&zeroDateTimeBehavior=convertToNull&rewriteBatchedStatements=true&tinyInt1isBit=false";

    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder().master("local").getOrCreate();
        JavaSparkContext javaSparkContext = JavaSparkContext.fromSparkContext(spark.sparkContext());

        Dataset<Row> tagCommentDF = spark.read().jdbc(JDBC_URL, "hbase_tag",
                getMysqlJDBCProperties());
        //获取 tag list
        List<Tag> tagList = tagCommentDF.toJavaRDD()
                .map(x -> new Tag(x.getInt(0), x.getString(1), x.getString(2), x.getInt(3), x.getString(4), x.getInt(5), x.getInt(6), x.getInt(7))).collect();








        javaSparkContext.close();
        spark.close();
    }

}
