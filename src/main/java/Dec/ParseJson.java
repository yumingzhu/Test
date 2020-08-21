package Dec;

import com.alibaba.fastjson.JSONObject;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;

public class ParseJson {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("StudentCount").setMaster("local");
        JavaSparkContext jsc = new  JavaSparkContext(conf);
        JavaRDD<String> javaRDD = jsc.textFile("G:\\test\\request.log.2018-09-05-05");
        JavaRDD<String> mapRDD1 = javaRDD.map(new Function<String, String>() {
            @Override
            public String call(String v1) throws Exception {
                String[] strings = v1.split("  ");
                if(strings.length>=1){
                    String json = strings[1];
                    try {
                        JSONObject jsonObject= JSONObject.parseObject(json);
                        Object id = jsonObject.get("id");
                        return id.toString();
                    } catch (Exception e) {
                        return null;
                    }

                }
                return  null;

            }
        }).filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String v1) throws Exception {
                return v1!=null?true:false;
            }
        });
        mapRDD1.foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println("s = " + s);
            }
        });
    }
}
