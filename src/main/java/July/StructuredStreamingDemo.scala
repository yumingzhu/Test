package July

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, SparkSession}

object StructuredStreamingDemo {
  def main(args: Array[String]): Unit = {
    val sparkSession: SparkSession = SparkSession.builder().master("local").appName("StructuredStreamingDemo").getOrCreate()
    val lines: DataFrame = sparkSession.readStream.format("socket").option("host", "192.168.2.52").option("port", "9999").load()
    import sparkSession.implicits._
    val words: Dataset[String] = lines.as[String].flatMap(_.split(" "))
    val worrdCounts = words.groupBy("value").count()
    val query = worrdCounts.writeStream.outputMode("complete").format("console").start()
    query.awaitTermination()


    sparkSession.stop()
  }




}
