package Dec;

import Dec.Accumulator.WorkAccumulator;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.Accumulator;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.SparkSession;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @Description TODO
 * @Author yumigzhu
 * @Date 2019/3/27 16:52
 */
public class FilterJob {
    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder().master("local").getOrCreate();
        JavaSparkContext javaSparkContext = JavaSparkContext.fromSparkContext(sparkSession.sparkContext());
        JavaRDD<String> javaRdd = javaSparkContext.textFile("G:\\bidLogs\\bid\\*");
        javaRdd.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String v1) throws Exception {
                JSONObject jsonObject = JSON.parseObject(v1);
                JSONArray impList = jsonObject.getJSONArray("impList");
                JSONObject imp = jsonObject.getJSONArray("impList").getJSONObject(0);
                String adSlotId = imp.getString("adSlotId");

                if(StringUtils.isNotBlank(adSlotId)){
                    return true;

                }
                return false;
            }
        }).coalesce(1).saveAsTextFile("G:\\bid_logs\\bid\\2019-03-27");

    }

}
