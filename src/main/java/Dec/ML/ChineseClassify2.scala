package Dec.ML

import org.ansj.recognition.impl.StopRecognition
import org.ansj.splitWord.analysis.ToAnalysis
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.classification.NaiveBayes
import org.apache.spark.ml.feature.{HashingTF, IDF, LabeledPoint, Tokenizer}
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.sql.{Row, SparkSession}

object ChineseClassify2 {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().appName("LogisticRegressionWithSGDDemo").master("local").getOrCreate()
    val sc = sparkSession.sparkContext;
    import sparkSession.implicits._
    //将原始数据映射到DataFrame中，字段category为分类编号，字段text为分好的词，以空格分隔
    val filter = new StopRecognition()
    filter.insertStopNatures("w") //过滤掉标点
    var js = sc.textFile("file:///G:/test/js/*")
      .map { x =>
        var str = if (x.length > 0)
          ToAnalysis.parse(x).recognition(filter).toStringWithOutNature(" ")
        str.toString
      }.map(RawDataRecord(0,_)).toDF("category","text")
    var nba = sc.textFile("file:///G:/test/nba/*")
      .map { x =>
        var str = if (x.length > 0)
          ToAnalysis.parse(x).recognition(filter).toStringWithOutNature(" ")
        str.toString
      }.map(RawDataRecord(1,_)).toDF("category","text")
    val training = nba.union(js)
    //将分好的词转换为数组
    var tokenizer = new Tokenizer().setInputCol("text").setOutputCol("words")
    //将每个词转换成Int型，并计算其在文档中的词频（TF）
    var hashingTF =
      new HashingTF().setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(100)
    var idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")

    var srcDF2 = sc.textFile("file:///G:/test/nba/*")
      .map { x =>
        var str = if (x.length > 0)
          ToAnalysis.parse(x).recognition(filter).toStringWithOutNature(" ")
        str.toString
      }.map(RawDataRecord(0,_)).toDF("category","text")

    val pipeline = new  Pipeline().setStages(Array(tokenizer,hashingTF,idf))
    var idfModel = pipeline.fit(training)
    var rescaledData = idfModel.transform(training)
    // re
    var trainDataRdd = rescaledData.select($"category",$"features").map {
      case Row(label: Int, features: Vector) =>
        LabeledPoint(label.toDouble, Vectors.dense(features.toArray))
    }
    //训练模型
    val model =new NaiveBayes().fit(trainDataRdd)

    //    //测试数据集，做同样的特征表示及格式转换


    val testrescaledData = idfModel.transform(srcDF2)
    var testDataRdd = testrescaledData.select("features").map {
      case Row( features: Vector) =>
        LabeledPoint(0.0, Vectors.dense(features.toArray))
    }
    val testpredictionAndLabel = model.transform(testDataRdd)
    testpredictionAndLabel.createTempView("demo")
    sparkSession.sql("select count(prediction) as  count from demo  group by prediction").show()
  }
  case class RawDataRecord(category: Integer, text: String)
  case class RawDataRecord2( text: String)
}
