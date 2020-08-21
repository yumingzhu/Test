package Dec

import java.io.{BufferedReader, InputStreamReader}
import java.util.zip.ZipInputStream

import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.TextInputFormat
import org.apache.spark.SparkContext
import org.apache.spark.input.PortableDataStream
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object Test_Dec {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder.master("local").getOrCreate
    val sc = sparkSession.sparkContext

    sparkSession.read.option("header",true).csv("file:///F:/test//train/train.csv").limit(100000).repartition(1).write
      .csv("file:///F:/test/ctrdata")



  }
}
