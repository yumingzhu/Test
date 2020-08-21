package Jan;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.SparkSession;
import org.elasticsearch.spark.rdd.api.java.JavaEsSpark;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * @author linhua
 * local[*]
 * test-i/test-t
 * {\"query\":{\"term\":{\"name\":\"hello123\"}}}
 * C:\Users\Administrator\Desktop\gpb\es
 * @date 2019/3/1 14:42
 */
public class EsReadHbase {
    public static void main(String[] args) {

        String master = args[0];
        String elasticIndex = args[1];
        String queryStr = args[2];
        String resPath = args[3];
        System.out.println(master+":"+queryStr+":"+resPath);
        long startTime = System.currentTimeMillis();
        SparkConf sparkConf = new SparkConf()
//                .setAppName("EsReadTotal")
//                .setMaster(master)
                .set("es.nodes", "192.168.1.7,192.168.1.8")
                .set("es.port", "9200")
//                .set("es.read.metadata", "true")
                .set("es.nodes.wan.only", "true");
        SparkSession spark = SparkSession.builder().config(sparkConf).getOrCreate();
        JavaSparkContext javaSparkContext = JavaSparkContext.fromSparkContext(spark.sparkContext());

        JavaPairRDD<String, String> esPairRDD = JavaEsSpark.esJsonRDD(javaSparkContext, elasticIndex, queryStr);
        JavaRDD<String> javaRDD = esPairRDD.map(new Function<Tuple2<String, String>, String>() {

            @Override
            public String call(Tuple2<String, String> v1) throws Exception {
                return v1._1;
            }
        });


        JavaRDD<List<String>> listJavaRDD = javaRDD.repartition(40).mapPartitions(new FlatMapFunction<Iterator<String>, List<String>>() {
            @Override
            public Iterator<List<String>> call(Iterator<String> stringIterator) throws Exception {
                Configuration conf = null;
                conf = HBaseConfiguration.create();
                conf.set("hbase.zookeeper.quorum", "192.168.1.7");
                conf.set("hbase.zookeeper.property.clientPort", "2181");

                Connection connection = null;
                connection = ConnectionFactory.createConnection(conf);
                String tableName = "aes_tag_data_bak";
                Table table = connection.getTable(TableName.valueOf(tableName));

                List<List<String>> res = new ArrayList<List<String>>();



                /**
                 * 批量获取hbase数据
                 */
                List<Get> getList = new ArrayList();
                while (stringIterator.hasNext()) {
                    String rowkey = stringIterator.next();
                    Get get = new Get(Bytes.toBytes(rowkey));
                    getList.add(get);
                    if(getList.size()==5000){

                        Result[] results = table.get(getList);//重点在这，直接查getList<Get>

                        for (Result result : results) {//对返回的结果集进行操作
                            List<String> values = new ArrayList<String>();
                            for (Cell kv : result.rawCells()) {

                                String value = Bytes.toString(CellUtil.cloneValue(kv));
                                values.add(value);
                            }
                            res.add(values);
                        }

                        getList.clear();
                    }
                }
                //取剩下小于5000的
                Result[] results = table.get(getList);//重点在这，直接查getList<Get>

                for (Result result : results) {//对返回的结果集进行操作
                    List<String> values = new ArrayList<String>();
                    for (Cell kv : result.rawCells()) {

                        String value = Bytes.toString(CellUtil.cloneValue(kv));
                        values.add(value);
                    }
                    res.add(values);
                }

                getList.clear();



                //一次拉取一条数据
//                while (stringIterator.hasNext()) {
//                    String rowkey = stringIterator.next();
//                    Get get = new Get(Bytes.toBytes(rowkey));
//                    Result result = table.get(get);
//                    List<String> values = new ArrayList<String>();
//                    for (Cell kv : result.rawCells()) {
//
//                        String value = Bytes.toString(CellUtil.cloneValue(kv));
//                        values.add(value);
//                    }
//                    res.add(values);
//                }

                connection.close();

                return res.iterator();
            }
        });

        JavaRDD<String> map = listJavaRDD.map(new Function<List<String>, String>() {
            @Override
            public String call(List<String> v1) throws Exception {
                StringBuffer sb = new StringBuffer();
                for (String s : v1) {
                    sb.append(s).append(",");
                }
                return sb.toString();
            }
        });

        map.saveAsTextFile(resPath);

        javaSparkContext.close();
        spark.close();

        long endTime = System.currentTimeMillis();
        System.out.println("consume 消耗多少秒: "+(endTime-startTime)/1000);

    }
}
