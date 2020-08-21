package Apr;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * @Description TODO
 * @Author yumigzhu
 * @Date 2019/4/15 17:53
 */
public class TestSpark {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf()
                                .setMaster("local")
				                .setAppName("SparkEsToHdfs");
		SparkSession spark = SparkSession.builder().config(sparkConf).getOrCreate();
		JavaSparkContext javaSparkContext = JavaSparkContext.fromSparkContext(spark.sparkContext());
        JavaPairRDD<String, String> packagesJavaRDD = javaSparkContext.parallelizePairs(Collections.EMPTY_LIST);
        JavaRDD<String> stringJavaRDD = javaSparkContext.textFile("G:\\test\\name.txt");
        stringJavaRDD.map(new Function<String, String>() {
            @Override
            public String call(String v1) throws Exception {
              if(StringUtils.isNotBlank(v1)){
                  String  pre="INSERT INTO `hbase_tag` (tag_id,name,pid,pids,status,type,output_length,update_date,create_date,del_flag) VALUES ('1', '";
                  String  end="', '0', '', '1', '0', '1', '2019-04-17 14:47:29', '2019-04-17 14:47:32', '1');";
                  return pre+v1+end;
              }
              return  null;
            }
        }).filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String v1) throws Exception {
                return v1!=null?true:false;
            }
        }).coalesce(1).saveAsTextFile("G:\\test\\sql");


    }
}
