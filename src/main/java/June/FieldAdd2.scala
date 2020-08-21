package June

import java.text.SimpleDateFormat

import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.ConnectionFactory
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Put, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ListBuffer

/**
  * Created by Administrator on 2019/6/25.
  */
object FieldAdd2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("regist-ct").setMaster("local")
    val sc = new SparkContext(conf)
    sc.setLogLevel("INFO")
    val hbaseConf = HBaseConfiguration.create()

    //读取hbase表cdr_regist中得数据
    hbaseConf.set(TableInputFormat.INPUT_TABLE, "cdr_regist")
    val rdd_cdr_regist = sc.newAPIHadoopRDD(
      hbaseConf,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result]
    ).cache() //数据缓存： 给下面要执行的各个方法，提供rdd
   var register_time=1L;
    //将hbase cdr_regist表中的数据根据regist_time排序
   rdd_cdr_regist.map(_._2).map(result => {
      val patient_id1 = Bytes.toString(result.getValue(Bytes.toBytes("f"), Bytes.toBytes("patient_id")))
      val visit_id = Bytes.toString(result.getValue(Bytes.toBytes("f"), Bytes.toBytes("visit_id")))
      val regist_time = getLongTime(Bytes.toString(result.getValue(Bytes.toBytes("f"), Bytes.toBytes("regist_time"))))
      (patient_id1, collection.mutable.ListBuffer((visit_id, regist_time)))
    }).reduceByKey((a,b)=>{
       a++=b
       a
   }).foreach(x=>{
     val value: ListBuffer[(String, Long)] = x._2.sortBy(_._2)
     println("你的操作")
   });



    //读取hbase  cdr_ct表数据
    hbaseConf.set(TableInputFormat.INPUT_TABLE, "cdr_ct")
    val rdd_cdr_ct = sc.newAPIHadoopRDD(
      hbaseConf,
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result]
    ).map(_._2).cache() //数据缓存： 给下面要执行的各个方法，提供rdd

    //  //广播rdd2
    //  val rdd2_broad = sc.broadcast(rdd_cdr_regist)
    //  //rdd数据变换==================================
    //  val rdd2 = rdd2_broad.value
    //  rdd2.foreach(result => {
    //    val dept_type = Bytes.toString(result.getValue(Bytes.toBytes("f"), Bytes.toBytes("dept_type")))
    //    val patient_id2 = Bytes.toString(result.getValue(Bytes.toBytes("f"), Bytes.toBytes("patient_id")))
    //    val report_time = getLongTime(Bytes.toString(result.getValue(Bytes.toBytes("f"), Bytes.toBytes("report_time"))))
    //
    //    //判断子表的patient, 时间 ==>插入visit_id
    //    if (patient_id2 == patient_id1 && report_time <= regist_time && report_time >=) {
    //      val put = new Put(result.getRow())
    //      put.addColumn(Bytes.toBytes("f"), Bytes.toBytes("visit_id"), Bytes.toBytes(visit_id))
    //      val conf = HBaseConfiguration.create
    //      val conn = ConnectionFactory.createConnection(conf)
    //
    //      //获取表
    //      val table = conn.getTable(TableName.valueOf("cdr_ct"))
    //      val table1 = table.asInstanceOf[HTable]
    //      table1.put(put)
    //      table1.close()
    //      println("--------put .........")
    //    }
    //  })
    //  })
  }
    val format1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    def getLongTime(timeStr: String): Long = {
      format1.parse(timeStr).getTime
    }

    def timeParse(timeStr: String): Unit = {
    }

}