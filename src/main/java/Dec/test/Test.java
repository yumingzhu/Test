package Dec.test;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * @Description TODO
 * @Author yumigzhu
 * @Date 2019/3/28 17:43
 */
public class Test {
	public static void main(String[] args) {

		SparkSession spark = SparkSession.builder().master("local").appName("winPriceProbability").getOrCreate();
		JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
        List<Tuple2<Integer,Integer>> list=new ArrayList<>();
        list.add(new Tuple2(1,1));
        list.add(new Tuple2(1,2));
        list.add(new Tuple2(1,3));
        list.add(new Tuple2(2,1));
        list.add(new Tuple2(2,2));
        list.add(new Tuple2(2,3));
        JavaRDD<Tuple2<Integer, Integer>> parallelize = jsc.parallelize(list);
        JavaPairRDD<Integer, Integer> integerIntegerJavaPairRDD = parallelize.mapToPair(new PairFunction<Tuple2<Integer, Integer>, Integer, Integer>() {

            @Override
            public Tuple2<Integer, Integer> call(Tuple2<Integer, Integer> integerIntegerTuple2) throws Exception {
                return integerIntegerTuple2;
            }
        });
        JavaPairRDD<Integer, Integer> integerIntegerJavaPairRDD1 = integerIntegerJavaPairRDD.aggregateByKey(0, new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        }, new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                System.out.println("----------------");
                return v1 + v2;
            }
        });
        integerIntegerJavaPairRDD1.foreach(new VoidFunction<Tuple2<Integer, Integer>>() {
            @Override
            public void call(Tuple2<Integer, Integer> v) throws Exception {
                System.out.println( v._1+"\t"+v._2);
            }
        });


	}
}
