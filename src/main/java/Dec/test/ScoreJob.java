package Dec.test;

import Dec.bean.Score;
import Dec.function.ScoreAvgUDAF;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class ScoreJob {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().master("local").appName("ScoreJobL").getOrCreate();
        spark.udf().register("ScoreAvg",new ScoreAvgUDAF());
        JavaSparkContext sc=JavaSparkContext.fromSparkContext(spark.sparkContext());
        JavaRDD<Score> scoreRDD = sc.textFile("G:\\test\\score.txt").map(new Function<String, Score>() {
            @Override
            public Score call(String v1) throws Exception {
                String[] split = v1.split(" ");
                return new Score(Integer.valueOf(split[0]), Integer.valueOf(split[1]));
            }
        });
        Dataset<Row> ds = spark.createDataFrame(scoreRDD, Score.class);
        ds.registerTempTable("score");
        spark.sql("select ScoreAvg(chinese,math) from score").show();
        spark.sql("select sum(chinese) ,sum(math) from score").show();

    }

}
