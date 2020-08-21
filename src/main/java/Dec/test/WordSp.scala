package Dec.test

import org.ansj.recognition.impl.StopRecognition
import org.ansj.splitWord.analysis.ToAnalysis
import org.apache.spark.{SparkConf, SparkContext}

object WordSp {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("input").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val filter = new StopRecognition()
    filter.insertStopNatures("w") //过滤掉标点

    val rdd=sc.textFile("G:\\test\\junshi.txt")
      .map { x =>
        var str = if (x.length > 0)
          ToAnalysis.parse(x).recognition(filter).toStringWithOutNature(" ")
        str.toString
      }.foreach(println)




  }
}
