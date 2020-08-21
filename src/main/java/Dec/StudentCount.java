package Dec;

import Dec.bean.Student;
import Dec.function.StudentComparator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Int;
import scala.Tuple2;

import java.util.List;
import java.util.function.Consumer;

public class StudentCount {
    public static void main(String[] args) {
       // SparkSession sparkSession = SparkSession.builder().appName("StudentCount").master("local").getOrCreate();
        SparkConf conf = new SparkConf().setAppName("StudentCount").setMaster("local");
        JavaSparkContext jsc = new  JavaSparkContext(conf);
        JavaRDD<String> javaRDD = jsc.textFile("G:\\test\\student.txt");

        List<Tuple2<Student, String>> stuCollect = javaRDD.mapToPair(new PairFunction<String, Student, String>() {
            @Override
            public Tuple2<Student, String> call(String s) throws Exception {
                String[] strings = s.split(" ");
                Student student = new Student.Builder().name(strings[0]).chinese(Integer.valueOf(strings[1])).math(Integer.valueOf(strings[2])).build();
                return new Tuple2<>(student, strings[0]);
            }
        }).sortByKey(false).collect();
        stuCollect.forEach(new Consumer<Tuple2<Student, String>>() {
            @Override
            public void accept(Tuple2<Student, String> stu) {
                Student student = stu._1;
                System.out.println(student.getName()+"\t"+student.getChinese()+"\t"+ student.getMath());
            }
        });


        //        JavaPairRDD<String, String> stuRDD = javaRDD.mapToPair(new PairFunction<String, String, String>() {
//            @Override
//            public Tuple2<String, String> call(String s) throws Exception {
//                String[] strings = s.split(" ");
//                return new Tuple2<String, String>( strings[1] + "_" + strings[2],strings[0]);
//            }
//        });
//
//        JavaPairRDD<String, String> studentRDD = stuRDD.sortByKey(new StudentComparator());
//        studentRDD.collect().forEach(new Consumer<Tuple2<String, String>>() {
//            @Override
//            public void accept(Tuple2<String, String> tuple2) {
//                String[] split = tuple2._1.split("_");
//                System.out.println(tuple2._2+"\t" +split[0]+"\t "+split[1]);
//            }
//        });

    }

}
