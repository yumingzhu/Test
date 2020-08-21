import java.util
import java.util.Properties

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{ConnectionFactory, Result}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.SaveMode
import org.apache.spark.{SparkConf, SparkContext}
import scala.collection.JavaConverters._

/**
  * 统计： 该hbase表的总记录条数， 写入到mysql
  */
object hbaseCounts {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    conf.setAppName("hbase_count").setMaster("local[*]")
    //根据表名， 读取数据
    val sc = new SparkContext(conf)
    val hbaseConf = HBaseConfiguration.create()
    val conn = ConnectionFactory.createConnection(hbaseConf)
    //创建： admin , table
    val admin = conn.getAdmin
    val tabNames = admin.listTableNames()

    val list = new util.ArrayList[(String, String, String)]()
    for (tbName <- tabNames) {
      val table = Bytes.toString(tbName.getName).toString
      if (!table.contains("KYLIN_")) { //排除kylin计算后的元数据表
        hbaseConf.set(TableInputFormat.INPUT_TABLE, table)
        val rdd = sc.newAPIHadoopRDD(
          hbaseConf,
          classOf[TableInputFormat],
          classOf[ImmutableBytesWritable],
          classOf[Result]
        )
        val totalCount = rdd.count().toString
        val time1 = System.currentTimeMillis().toString

        list.add((table, totalCount, time1))
      }
    }
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    sqlContext.createDataFrame(list.asScala).toDF("hbase_count", "creat_time", "table_name").write
//      .registerTempTable("hbase_count")
    //求占比
    var result = sqlContext.sql("select *  from  hbase_count ")
    result.show()
    //配置mysql得连接参数
    val prop = new Properties()
    prop.setProperty("driver", "com.mysql.jdbc.Driver")
    prop.setProperty("user", "spark")
    prop.setProperty("password", "spark12345")
    result.write.mode(SaveMode.Overwrite).jdbc("jdbc:mysql://218.245.1.135:3306/spark", "hbase_counts", prop)



  }
}