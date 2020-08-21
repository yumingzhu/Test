package Dec;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import Dec.function.WorkUDF;
import Dec.function.WorkcountUDAF;

public class WorkCountSQL3 {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder().master("local").appName("WorkCount JAVASQL").getOrCreate();
        spark.udf().register("ToUpper",new WorkUDF(),DataTypes.StringType);
        spark.udf().register("StrCount",new WorkcountUDAF());

        JavaSparkContext javaSparkContext = new JavaSparkContext(spark.sparkContext());
        JavaRDD<String> stringJavaRDD = javaSparkContext.textFile("G:\\test\\demo.txt");
        JavaRDD<Row> strRDD =stringJavaRDD.flatMap(line -> Arrays.asList(line.split(" ")).iterator()).map(line->RowFactory.create(line));
       List<StructField>  structFields=new ArrayList<>();
       structFields.add(DataTypes.createStructField("work",DataTypes.StringType,true));
        StructType schema=DataTypes.createStructType(structFields);
        Dataset<Row> dataFrame = spark.createDataFrame(strRDD, schema);
        dataFrame.registerTempTable("works");
      //    spark.sql("select ToUpper(work) from works").show();
         spark.sql("select work ,StrCount(work) as count from works group by work").show();
    }

}
