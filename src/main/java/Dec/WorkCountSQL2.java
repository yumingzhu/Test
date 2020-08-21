package Dec;

import Dec.bean.Person;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class WorkCountSQL2 {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().master("local").appName("WorkCount JAVASQL").getOrCreate();
        JavaSparkContext javaSparkContext = new JavaSparkContext(spark.sparkContext());

        JavaRDD<String> stringJavaRDD = javaSparkContext.textFile("G:\\test\\demo.txt");
        JavaRDD<Person> strRDD =stringJavaRDD.flatMap(line -> Arrays.asList(line.split(" ")).iterator()).map(line->{
                    Person p=new  Person.Builder().name(null).build();
                    return  p;
                });
        //System.out.println("strRDD.count() = " + strRDD.count());
        Dataset<Row> dataFrame = spark.createDataFrame(strRDD, Person.class);
        dataFrame.registerTempTable("person");
        spark.sql("select name,count(1) as count from person group by name").show();

    }
}
