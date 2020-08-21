import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.java.DataSet;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.HashMap;
import java.util.Map;

/**
 * @Description TODO
 * @Author yumigzhu
 * @Date 2019/2/28 14:41
 */
public class SparkOnSolr {
	public static void main(String[] args) {
        //        SparkConf sparkConf = new SparkConf()
        //                .setAppName("Test");
        //
        //        SparkSession sparkSession = SparkSession.builder().master("local").appName("TestSparkOnSolrByCondition").getOrCreate();
        //        Map<String,String> options = new HashMap<>();
        //       options.put("collection" , "aes_tag_data");
        //        options.put("zkhost", "192.168.1.7:2181,192.168.1.8:2181,192.168.1.9:2181/solr");
        //        Dataset<Row> df = sparkSession.read().format("solr").options(options).load();
        //        df.printSchema();
        //        df.show(20);
        //
        //
        //        sparkSession.close();
        String oldDate = "yk20190729:3,yk20190729:2,yk20190806:1";
        String date = "yk20190728";
        if (StringUtils.isNotBlank(oldDate)) {
            String[] split = oldDate.split(",");
            String needReplac = null;
            String value = null;
            for (String temp : split) {
                String[] strings = temp.split(":");
                if (strings[0].equals(date)) {
                    needReplac = temp;
                    value = date + ":" + (Integer.valueOf(strings[1]) + 1);
                    break;
                }
            }
            if (needReplac == null) {
                oldDate = oldDate + "," + date + ":" + 1;
            } else {
                oldDate = oldDate.replace(needReplac, value);
            }

        } else {
            oldDate = date + ":" + 1;
        }
        System.out.println(oldDate);

	}
}
