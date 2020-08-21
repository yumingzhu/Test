package Jan;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.Accumulator;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.SparkSession;

import Dec.Accumulator.WorkAccumulator;
import scala.Tuple2;

public class WorkCount {
    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder().master("local[*]").getOrCreate();
        JavaSparkContext javaSparkContext = JavaSparkContext.fromSparkContext(sparkSession.sparkContext());
        Accumulator<Integer> count = javaSparkContext.accumulator(0);
        JavaRDD<String> javaRdd = javaSparkContext.textFile("G:\\test\\demo.txt");

        javaRdd.foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                count.add(0);

                System.out.println("s = " + s);


            }
        });

        System.out.println(  javaRdd.distinct().count());
    }
}