package Apr;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.SparkSession;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

/**
 * @Description TODO
 * @Author yumigzhu
 * @Date 2019/4/19 18:37
 */
public class Test {
	public static void main(String[] args) {
		String target = new String(new char[5]).replace('\0', '0');
		String  str="A7FCFC6B5269BDCCE571798D618EA219A68B96CB87A0E21080C2E758D23E4CE9";
		System.out.println(str.length());
		System.out.println("target = " + target);
	}
   public  static void testspark(){
	   SparkConf sparkConf = new SparkConf().setMaster("local").setAppName("SparkEsToHdfs");
	   SparkSession spark = SparkSession.builder().config(sparkConf).getOrCreate();
	   JavaSparkContext javaSparkContext = JavaSparkContext.fromSparkContext(spark.sparkContext());
	   JavaPairRDD<String, String> packagesJavaRDD = javaSparkContext.parallelizePairs(Collections.EMPTY_LIST);
	   JavaRDD<String> stringJavaRDD = javaSparkContext.textFile("G:/mywork/bigdata/");
	   stringJavaRDD.flatMap(new FlatMapFunction<String, String>() {
		   @Override
		   public Iterator<String> call(String s) throws Exception {
			   List list=new ArrayList();
			   for (int i = 0; i <100000 ; i++) {
				   list.add(s);
			   }
			   return list.iterator();
		   }
	   }).saveAsTextFile("G:/mywork/bigdata");
   }

}
