package Apr;

import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @Description TODO
 * @Author yumigzhu
 * @Date 2019/4/29 18:08
 */
public class JavaCoustomPartitioner {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf()
                .setMaster("local")
                .setAppName("SparkEsToHdfs");
        SparkSession spark = SparkSession.builder().config(sparkConf).getOrCreate();
        JavaSparkContext javaSparkContext = JavaSparkContext.fromSparkContext(spark.sparkContext());
        List<Integer>  list=new ArrayList<>();
        for (int i = 0; i <11 ; i++) {
            list.add(i);
        }
        JavaRDD<Integer> rdd = javaSparkContext.parallelize(list);
        rdd.mapToPair(new PairFunction<Integer, Integer, Integer>() {
            @Override
            public Tuple2<Integer, Integer> call(Integer integer) throws Exception {
                return new Tuple2<>(integer,1);
            }
        }).partitionBy(new MyPartitioner(2)).foreach(new VoidFunction<Tuple2<Integer, Integer>>() {
            @Override
            public void call(Tuple2<Integer, Integer> integerIntegerTuple2) throws Exception {
                int i = TaskContext.get().partitionId();
                System.out.println(integerIntegerTuple2._1+"\t"+i);
            }
        });
    }

}
class  MyPartitioner extends Partitioner{

    public MyPartitioner(int partitions) {
        this.partitions = partitions;
    }

    private int partitions;


    public void setPartitions(int partitions) {
        this.partitions = partitions;
    }

    @Override
    public int numPartitions() {
        return partitions;
    }

    @Override
    public int getPartition(Object key) {
        return Integer.valueOf(key.toString()) % numPartitions();
    }
}