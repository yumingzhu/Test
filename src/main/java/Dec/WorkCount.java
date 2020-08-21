package Dec;

import Dec.Accumulator.WorkAccumulator;
import Dec.function.OneToTWO;
import org.apache.spark.Accumulator;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;

public class WorkCount {
    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder().master("local[*]").getOrCreate();
        JavaSparkContext javaSparkContext = JavaSparkContext.fromSparkContext(sparkSession.sparkContext());
        Accumulator<Integer> count = javaSparkContext.accumulator(0, new WorkAccumulator());
        JavaRDD<String> javaRdd = javaSparkContext.textFile("G:\\test\\demo.txt");

//        JavaPairRDD<String, Integer> flatMapRDD = javaRdd.flatMapToPair(new OneToTWO());
//        JavaPairRDD<String, Integer> reduceRDD = flatMapRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
//            @Override
//            public Integer call(Integer v1, Integer v2) throws Exception {
//                return v1 + v2;
//            }
//        });
//        reduceRDD.collect().forEach(new Consumer<Tuple2<String, Integer>>() {
//            @Override
//            public void accept(Tuple2<String, Integer> stringIntegerTuple2) {
//                System.out.println(stringIntegerTuple2._1 + "count " + stringIntegerTuple2._2);
//            }
//        });


        javaRdd.flatMapToPair(new PairFlatMapFunction<String, String, Integer>() {
            @Override
            public Iterator<Tuple2<String, Integer>> call(String s) throws Exception {
                List<Tuple2<String, Integer>> list = new ArrayList<>();
                count.add(1);
                for (String s1 : s.split(" ")) {
                    list.add(new Tuple2<String, Integer>(s1, 1));
                }
                return list.iterator();
            }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        }).foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                System.out.println(stringIntegerTuple2._1 + "count " + stringIntegerTuple2._2);
            }
        });

        System.out.println("count = " + count.value());
//        javaRdd.flatMap(new FlatMapFunction<String, String>() {
//            @Override
//            public Iterator<String> call(String s) throws Exception {
//                return Arrays.asList(s.split(" ")).iterator();
//            }
//        }).mapToPair(new PairFunction<String, String,Integer>() {
//            @Override
//            public Tuple2<String, Integer> call(String s) throws Exception {
//                return new Tuple2<String, Integer>(s,1);
//            }
//        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
//            @Override
//            public Integer call(Integer v1, Integer v2) throws Exception {
//                return v1+v2;
//            }
//        }).foreach(new VoidFunction<Tuple2<String, Integer>>() {
//            @Override
//            public void call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
//                System.out.println(stringIntegerTuple2._1 + "count " + stringIntegerTuple2._2);
//            }
//        });

    }
}