package Dec.ML



import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.HashingTF
import org.apache.spark.ml.feature.IDF
import org.apache.spark.ml.feature.LabeledPoint
import org.apache.spark.ml.feature.Tokenizer
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.classification.NaiveBayes
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.sql.{Row, SparkSession}
object ChineseClassify {
  def main(args: Array[String]): Unit = {
    val sparkSession = SparkSession.builder().appName("LogisticRegressionWithSGDDemo").master("local").getOrCreate()
    val sc = sparkSession.sparkContext;
    import sparkSession.implicits._
    //将原始数据映射到DataFrame中，字段category为分类编号，字段text为分好的词，以空格分隔
    var training = sc.textFile("G:\\test\\1.txt").map {
      x =>
        var data = x.split(",")
        RawDataRecord(data(0),data(1))
    }.toDF("category","text")


    //将分好的词转换为数组
    var tokenizer = new Tokenizer().setInputCol("text").setOutputCol("words")
    //将每个词转换成Int型，并计算其在文档中的词频（TF）
    var hashingTF =
      new HashingTF().setInputCol("words").setOutputCol("rawFeatures").setNumFeatures(100)
    var idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")

    var srcDF2 = sc.textFile("file:///G:/test/2.txt").map(RawDataRecord2(_)).toDF("text")

    val pipeline = new  Pipeline().setStages(Array(tokenizer,hashingTF,idf))
    var idfModel = pipeline.fit(training)
    var rescaledData = idfModel.transform(training)
    // rescaledData.show(false)
    //
    rescaledData.select($"category", $"words", $"features").show(false)
    var trainDataRdd = rescaledData.select($"category",$"features").map {
      case Row(label: String, features: Vector) =>
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
    testpredictionAndLabel.show(false)
    //评估模型
    val evaluator = new MulticlassClassificationEvaluator()
      .setLabelCol("label")
      .setPredictionCol("prediction")
      .setMetricName("accuracy")
    val accuracy = evaluator.evaluate(testpredictionAndLabel)
    println("准确率:"+accuracy)

  }
  case class RawDataRecord(category: String, text: String)
  case class RawDataRecord2( text: String)
}
