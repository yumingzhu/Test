package July

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.{Result, Scan}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.protobuf.ProtobufUtil
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos
import org.apache.hadoop.hbase.util.{Base64, Bytes}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

/**
  * Description   获取24 岁一下 ， 和46岁以上的人数
  * Author  余明珠
  * Date 2019/6/27
  */
object TagAgeJob {
  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder().appName("TagAgeJob").getOrCreate()
    val sc = sparkSession.sparkContext

    val accum = new TagAccumulator
    sc.register(accum, "TagAccum")


//    val configuration: Configuration = HBaseUtils.getConfiguration
    val scan: Scan = new Scan

    //        将条件放入到scan 中  去查询记录
    scan.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("qm_tag1"));
    scan.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("qm_tag2"));

    scan.setCaching(10000)
    scan.setBatch(35)


  }

}
