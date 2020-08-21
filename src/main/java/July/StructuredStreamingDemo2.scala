package July

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object StructuredStreamingDemo2 {
  def main(args: Array[String]): Unit = {
    val sparkSession: SparkSession = SparkSession.builder().master("local").appName("StructuredStreamingDemo").getOrCreate()
    val lines: DataFrame = sparkSession.readStream.format("socket").option("host", "192.168.2.52").option("port", "9999").load()
    import sparkSession.implicits._

    //基于窗口进行分组，
    val userSchema = new StructType().add("name", "string").add("age", "integer")
    val words: Dataset[Log] = lines.as[String].map(line => Log(line.split(",")(0), line.split(",")(1)))


//    val worrdCounts = words.groupBy(window($"timestamp", "10 minutes", "5 minutes"), $"word").count()
//
//    val query = worrdCounts.writeStream.outputMode("update").format("console").start()
//    query.awaitTermination()


    sparkSession.stop()
  }

  case class Log(time: String, word: String)

}
