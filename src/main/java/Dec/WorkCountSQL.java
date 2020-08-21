package Dec;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class WorkCountSQL {
    public static void main(String[] args) {
        System.out.println("args = " + args[0]);
        SparkSession spark = SparkSession.builder().master("local").appName("WorkCount JAVASQL").getOrCreate();
        JavaSparkContext javaSparkContext = new JavaSparkContext(spark.sparkContext());
        JavaRDD<String> stringJavaRDD = javaSparkContext.textFile("hdfs://127.0.0.1:9000//test/demo.txt");
        JavaRDD<Row> strRDD =stringJavaRDD.flatMap(line -> Arrays.asList(line.split(" ")).iterator()).map(line->RowFactory.create(line));
       List<StructField>  structFields=new ArrayList<>();
       structFields.add(DataTypes.createStructField("work",DataTypes.StringType,true));
        StructType schema=DataTypes.createStructType(structFields);

        Dataset<Row> dataFrame = spark.createDataFrame(strRDD, schema);
        dataFrame.registerTempTable("works");
        spark.catalog().cacheTable("works");
//        spark.catalog().listTables().show();
        spark.sql("select work,count(1) as count from works group by work").show();
        spark.sql("select work,count(1) as count from works group by work").coalesce(1).toJavaRDD()
                .saveAsTextFile("hdfs://127.0.0.1:9000//laucher.txt");

        javaSparkContext.close();
        spark.close();

    }
}
