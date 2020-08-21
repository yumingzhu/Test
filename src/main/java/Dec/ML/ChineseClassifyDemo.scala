package Dec.ML

import Dec.ML.ChineseClassify.RawDataRecord
import org.apache.spark.sql.SparkSession

object ChineseClassifyDemo {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().appName("LogisticRegressionWithSGDDemo").master("local").getOrCreate()
    val sc = sparkSession.sparkContext;
    import sparkSession.implicits._




  }
}
