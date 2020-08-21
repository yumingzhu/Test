package javaTest

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

object SparkbySolr {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setMaster("local")
      .setAppName("Test")
    // 创建 spark context
    val sc = new SparkContext(sparkConf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val options = Map(
      "collection" -> "aes_tag_data",
      "zkhost" -> "192.168.1.7:2182,192.168.1.8:2182,192.168.1.9:2182/solrcloud")
    val df = sqlContext.read.format("solr").options(options).load

    df.printSchema

    df.collect.foreach(println)

    sc.stop

  }
}
